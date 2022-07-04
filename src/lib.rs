use itertools::Itertools;
use log::info;
use solana_runtime::snapshot_utils::SNAPSHOT_STATUS_CACHE_FILENAME;
use std::cell::RefCell;
use std::ffi::OsStr;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read};
use std::path::{Component, Path, PathBuf};
use std::pin::Pin;
use std::rc::Rc;
use std::str::FromStr;
use std::time::Instant;
use tar::{Archive, Entries, Entry};
use thiserror::Error;

pub mod append_vec;
pub mod solana;

use crate::append_vec::{AppendVec, StoredAccountMeta};
use crate::solana::{
    deserialize_from, AccountsDbFields, DeserializableVersionedBank,
    SerializableAccountStorageEntry,
};

const SNAPSHOTS_DIR: &str = "snapshots";

#[derive(Error, Debug)]
pub enum SnapshotError {
    #[error("{0}")]
    IOError(#[from] std::io::Error),
    #[error("Failed to deserialize: {0}")]
    BincodeError(#[from] bincode::Error),
    #[error("Missing status cache")]
    NoStatusCache,
    #[error("No snapshot manifest file found")]
    NoSnapshotManifest,
    #[error("Unexpected AppendVec")]
    UnexpectedAppendVec,
}

pub type Result<T> = std::result::Result<T, SnapshotError>;

pub trait SnapshotLoader: Sized {
    fn iter(&mut self) -> Box<dyn Iterator<Item = Result<StoredAccountMetaHandle>> + '_>;
}

/// Loads account data from snapshots that were unarchived to a file system.
pub struct UnpackedSnapshotLoader {
    root: PathBuf,
    accounts_db_fields: AccountsDbFields<SerializableAccountStorageEntry>,
}

impl SnapshotLoader for UnpackedSnapshotLoader {
    fn iter(&mut self) -> Box<dyn Iterator<Item = Result<StoredAccountMetaHandle>> + '_> {
        Box::new(self.unboxed_iter())
    }
}

impl UnpackedSnapshotLoader {
    pub fn open(path: &Path, progress_tracking: Box<dyn ReadProgressTracking>) -> Result<Self> {
        let snapshots_dir = path.join(SNAPSHOTS_DIR);
        let status_cache = snapshots_dir.join(SNAPSHOT_STATUS_CACHE_FILENAME);
        if !status_cache.is_file() {
            return Err(SnapshotError::NoStatusCache);
        }

        let snapshot_files = snapshots_dir.read_dir()?;

        let snapshot_file_path = snapshot_files
            .filter_map(|entry| entry.ok())
            .find(|entry| u64::from_str(&entry.file_name().to_string_lossy()).is_ok())
            .map(|entry| entry.path().join(entry.file_name()))
            .ok_or(SnapshotError::NoSnapshotManifest)?;

        info!("Opening snapshot manifest: {:?}", snapshot_file_path);
        let snapshot_file = OpenOptions::new().read(true).open(&snapshot_file_path)?;
        let snapshot_file_len = snapshot_file.metadata()?.len();

        let snapshot_file = progress_tracking.new_read_progress_tracker(
            &snapshot_file_path,
            Box::new(snapshot_file),
            snapshot_file_len,
        );
        let mut snapshot_file = BufReader::new(snapshot_file);

        let pre_unpack = Instant::now();
        let versioned_bank: DeserializableVersionedBank = deserialize_from(&mut snapshot_file)?;
        drop(versioned_bank);
        let versioned_bank_post_time = Instant::now();

        let accounts_db_fields: AccountsDbFields<SerializableAccountStorageEntry> =
            deserialize_from(&mut snapshot_file)?;
        let accounts_db_fields_post_time = Instant::now();
        drop(snapshot_file);

        info!(
            "Read bank fields in {:?}",
            versioned_bank_post_time - pre_unpack
        );
        info!(
            "Read accounts DB fields in {:?}",
            accounts_db_fields_post_time - versioned_bank_post_time
        );

        Ok(UnpackedSnapshotLoader {
            root: path.to_path_buf(),
            accounts_db_fields,
        })
    }

    pub fn unboxed_iter(&self) -> impl Iterator<Item = Result<StoredAccountMetaHandle>> + '_ {
        std::iter::once(self.iter_streams())
            .flatten_ok()
            .flatten_ok()
            .map_ok(|stream| append_vec_iter(Rc::new(stream)))
            .flatten_ok()
    }

    fn iter_streams(&self) -> Result<impl Iterator<Item = Result<AppendVec>> + '_> {
        let accounts_dir = self.root.join("accounts");
        Ok(accounts_dir
            .read_dir()?
            .filter_map(|f| f.ok())
            .filter_map(|f| {
                let name = f.file_name();
                parse_append_vec_name(&f.file_name()).map(move |parsed| (parsed, name))
            })
            .map(move |((slot, version), name)| {
                self.open_append_vec(slot, version, &accounts_dir.join(name))
            }))
    }

    fn open_append_vec(&self, slot: u64, id: u64, path: &Path) -> Result<AppendVec> {
        let known_vecs = self
            .accounts_db_fields
            .0
            .get(&slot)
            .map(|v| &v[..])
            .unwrap_or(&[]);
        let known_vec = known_vecs.iter().find(|entry| entry.id == (id as usize));
        let known_vec = match known_vec {
            None => return Err(SnapshotError::UnexpectedAppendVec),
            Some(v) => v,
        };

        Ok(AppendVec::new_from_file(
            path,
            known_vec.accounts_current_len,
        )?)
    }
}

/// Loads account data from a .tar.zst stream.
pub struct ArchiveSnapshotLoader<Source>
where
    Source: Read + Unpin + 'static,
{
    accounts_db_fields: AccountsDbFields<SerializableAccountStorageEntry>,
    _archive: Pin<Box<Archive<zstd::Decoder<'static, BufReader<Source>>>>>,
    entries: Option<Entries<'static, zstd::Decoder<'static, BufReader<Source>>>>,
}

impl<Source> SnapshotLoader for ArchiveSnapshotLoader<Source>
where
    Source: Read + Unpin + 'static,
{
    fn iter(&mut self) -> Box<dyn Iterator<Item = Result<StoredAccountMetaHandle>> + '_> {
        Box::new(self.unboxed_iter())
    }
}

impl<Source> ArchiveSnapshotLoader<Source>
where
    Source: Read + Unpin + 'static,
{
    pub fn from_reader(source: Source) -> Result<Self> {
        let tar_stream = zstd::stream::read::Decoder::new(source)?;
        let mut archive = Box::pin(Archive::new(tar_stream));

        // This is safe as long as we guarantee that entries never gets accessed past drop.
        let archive_static = unsafe { &mut *((&mut *archive) as *mut Archive<_>) };
        let mut entries = archive_static.entries()?;

        // Search for snapshot manifest.
        let mut snapshot_file: Option<Entry<_>> = None;
        for entry in entries.by_ref() {
            let entry = entry?;
            let path = entry.path()?;
            if Self::is_snapshot_manifest_file(&path) {
                snapshot_file = Some(entry);
                break;
            } else if Self::is_appendvec_file(&path) {
                // TODO Support archives where AppendVecs precede snapshot manifests
                return Err(SnapshotError::UnexpectedAppendVec);
            }
        }
        let snapshot_file = snapshot_file.ok_or(SnapshotError::NoSnapshotManifest)?;
        //let snapshot_file_len = snapshot_file.size();
        let snapshot_file_path = snapshot_file.path()?.as_ref().to_path_buf();

        info!("Opening snapshot manifest: {:?}", &snapshot_file_path);
        let mut snapshot_file = BufReader::new(snapshot_file);

        let pre_unpack = Instant::now();
        let versioned_bank: DeserializableVersionedBank = deserialize_from(&mut snapshot_file)?;
        drop(versioned_bank);
        let versioned_bank_post_time = Instant::now();

        let accounts_db_fields: AccountsDbFields<SerializableAccountStorageEntry> =
            deserialize_from(&mut snapshot_file)?;
        let accounts_db_fields_post_time = Instant::now();
        drop(snapshot_file);

        info!(
            "Read bank fields in {:?}",
            versioned_bank_post_time - pre_unpack
        );
        info!(
            "Read accounts DB fields in {:?}",
            accounts_db_fields_post_time - versioned_bank_post_time
        );

        Ok(ArchiveSnapshotLoader {
            _archive: archive,
            accounts_db_fields,
            entries: Some(entries),
        })
    }

    pub fn unboxed_iter(&mut self) -> impl Iterator<Item = Result<StoredAccountMetaHandle>> + '_ {
        self.iter_streams()
            .map_ok(|stream| append_vec_iter(Rc::new(stream)))
            .flatten_ok()
    }

    fn iter_streams(&mut self) -> impl Iterator<Item = Result<AppendVec>> + '_ {
        self.entries
            .take()
            .into_iter()
            .flatten()
            .filter_map(|entry| {
                let mut entry = match entry {
                    Ok(x) => x,
                    Err(e) => return Some(Err(e.into())),
                };
                let path = match entry.path() {
                    Ok(x) => x,
                    Err(e) => return Some(Err(e.into())),
                };
                let (slot, id) = path.file_name().and_then(parse_append_vec_name)?;
                Some(self.process_entry(&mut entry, slot, id))
            })
    }

    fn process_entry(
        &self,
        entry: &mut Entry<'static, zstd::Decoder<'static, BufReader<Source>>>,
        slot: u64,
        id: u64,
    ) -> Result<AppendVec> {
        let known_vecs = self
            .accounts_db_fields
            .0
            .get(&slot)
            .map(|v| &v[..])
            .unwrap_or(&[]);
        let known_vec = known_vecs.iter().find(|entry| entry.id == (id as usize));
        let known_vec = match known_vec {
            None => return Err(SnapshotError::UnexpectedAppendVec),
            Some(v) => v,
        };
        Ok(AppendVec::new_from_reader(
            entry,
            known_vec.accounts_current_len,
        )?)
    }

    fn is_snapshot_manifest_file(path: &Path) -> bool {
        let mut components = path.components();
        if components.next() != Some(Component::Normal("snapshots".as_ref())) {
            return false;
        }
        let slot_number_str_1 = match components.next() {
            Some(Component::Normal(slot)) => slot,
            _ => return false,
        };
        // Check if slot number file is valid u64.
        if slot_number_str_1
            .to_str()
            .and_then(|s| s.parse::<u64>().ok())
            .is_none()
        {
            return false;
        }
        let slot_number_str_2 = match components.next() {
            Some(Component::Normal(slot)) => slot,
            _ => return false,
        };
        components.next().is_none() && slot_number_str_1 == slot_number_str_2
    }

    fn is_appendvec_file(path: &Path) -> bool {
        let mut components = path.components();
        if components.next() != Some(Component::Normal("accounts".as_ref())) {
            return false;
        }
        let name = match components.next() {
            Some(Component::Normal(c)) => c,
            _ => return false,
        };
        components.next().is_none() && parse_append_vec_name(name).is_some()
    }
}

impl ArchiveSnapshotLoader<File> {
    pub fn open(path: &Path) -> Result<Self> {
        Self::from_reader(File::open(path)?)
    }
}

fn parse_append_vec_name(name: &OsStr) -> Option<(u64, u64)> {
    let name = name.to_str()?;
    let mut parts = name.splitn(2, '.');
    let slot = u64::from_str(parts.next().unwrap_or(""));
    let id = u64::from_str(parts.next().unwrap_or(""));
    match (slot, id) {
        (Ok(slot), Ok(version)) => Some((slot, version)),
        _ => None,
    }
}

fn append_vec_iter(append_vec: Rc<AppendVec>) -> impl Iterator<Item = StoredAccountMetaHandle> {
    let mut offsets = Vec::<usize>::new();
    let mut offset = 0usize;
    loop {
        match append_vec.get_account(offset) {
            None => break,
            Some((_, next_offset)) => {
                offsets.push(offset);
                offset = next_offset;
            }
        }
    }
    let append_vec = Rc::clone(&append_vec);
    offsets
        .into_iter()
        .map(move |offset| StoredAccountMetaHandle::new(Rc::clone(&append_vec), offset))
}

pub struct StoredAccountMetaHandle {
    append_vec: Rc<AppendVec>,
    offset: usize,
}

impl StoredAccountMetaHandle {
    pub fn new(append_vec: Rc<AppendVec>, offset: usize) -> StoredAccountMetaHandle {
        Self { append_vec, offset }
    }

    pub fn access(&self) -> Option<StoredAccountMeta<'_>> {
        Some(self.append_vec.get_account(self.offset)?.0)
    }
}

pub trait ReadProgressTracking {
    fn new_read_progress_tracker(
        &self,
        path: &Path,
        rd: Box<dyn Read>,
        file_len: u64,
    ) -> Box<dyn Read>;
}

struct NullReadProgressTracking {}

impl ReadProgressTracking for NullReadProgressTracking {
    fn new_read_progress_tracker(&self, _: &Path, rd: Box<dyn Read>, _: u64) -> Box<dyn Read> {
        rd
    }
}

struct RefCellRead<T: Read> {
    rd: RefCell<T>,
}

impl<T: Read> Read for RefCellRead<T> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.rd
            .try_borrow_mut()
            .map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "attempted to read archive concurrently",
                )
            })
            .and_then(|mut rd| rd.read(buf))
    }
}
