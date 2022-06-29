use indicatif::{ProgressBar, ProgressStyle};
use itertools::Itertools;
use log::debug;
use ouroboros::self_referencing;
use solana_runtime::snapshot_utils::SNAPSHOT_STATUS_CACHE_FILENAME;
use std::ffi::OsString;
use std::fs::OpenOptions;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Instant;
use thiserror::Error;

mod append_vec;
pub mod solana;

use crate::append_vec::{AppendVec, AppendVecAccountsIter, StoredAccountMeta};
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

pub struct UnpackedSnapshotLoader {
    root: PathBuf,
    accounts_db_fields: AccountsDbFields<SerializableAccountStorageEntry>,
}

impl UnpackedSnapshotLoader {
    pub fn open<P>(path: P) -> Result<Self, SnapshotError>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref();

        let snapshots_dir = path.join(SNAPSHOTS_DIR);
        let status_cache = snapshots_dir.join(SNAPSHOT_STATUS_CACHE_FILENAME);
        if !status_cache.is_file() {
            return Err(SnapshotError::NoStatusCache);
        }

        let snapshot_files = snapshots_dir.read_dir()?;

        let snapshot_file = snapshot_files
            .filter_map(|entry| entry.ok())
            .filter(|entry| u64::from_str(&entry.file_name().to_string_lossy()).is_ok())
            .next()
            .map(|entry| entry.path().join(entry.file_name()))
            .ok_or(SnapshotError::NoSnapshotManifest)?;

        debug!("Opening snapshot manifest: {:?}", &snapshot_file);
        let snapshot_file = OpenOptions::new().read(true).open(snapshot_file)?;
        let snapshot_file_len = snapshot_file.metadata()?.len();

        let pb = ProgressBar::new(snapshot_file_len).with_style(
            ProgressStyle::with_template(
                "{spinner:.green} [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({percent}%)",
            )
            .unwrap()
            .progress_chars("#>-"),
        );
        let snapshot_file = pb.wrap_read(snapshot_file);

        let mut snapshot_file = BufReader::new(snapshot_file);

        let pre_unpack = Instant::now();
        let versioned_bank: DeserializableVersionedBank = deserialize_from(&mut snapshot_file)?;
        drop(versioned_bank);
        let versioned_bank_post_time = Instant::now();

        let accounts_db_fields: AccountsDbFields<SerializableAccountStorageEntry> =
            deserialize_from(&mut snapshot_file)?;
        let accounts_db_fields_post_time = Instant::now();

        pb.finish();

        debug!(
            "Read bank fields in {:?}",
            versioned_bank_post_time - pre_unpack
        );
        debug!(
            "Read accounts DB fields in {:?}",
            accounts_db_fields_post_time - versioned_bank_post_time
        );

        Ok(UnpackedSnapshotLoader {
            root: path.to_path_buf(),
            accounts_db_fields,
        })
    }

    fn iter(&self) -> Result<UnpackedSnapshotLoaderIter<'_, impl Iterator<Item = Result<OwnedAppendVecAccountsIter, SnapshotError>> + '_>, SnapshotError> {
        let streams = self.iter_streams()?;
        UnpackedSnapshotLoaderIter::new(self, streams)
    }

    fn iter_streams(
        &self,
    ) -> Result<
        impl Iterator<Item = Result<OwnedAppendVecAccountsIter, SnapshotError>> + '_,
        SnapshotError,
    > {
        let accounts_dir = self.root.join("accounts");
        Ok(accounts_dir
            .read_dir()?
            .filter_map(|f| f.ok())
            .filter_map(|f| {
                let name = f.file_name();
                parse_append_vec_name(&f.file_name()).map(move |parsed| (parsed, name))
            })
            .map(move |((slot, version), name)| {
                self.stream_log(slot, version, &accounts_dir.join(name))
            }))
    }

    fn stream_log(
        &self,
        slot: u64,
        id: u64,
        path: &Path,
    ) -> Result<OwnedAppendVecAccountsIter, SnapshotError> {
        let known_vecs = self
            .accounts_db_fields
            .0
            .get(&slot)
            .map(|v| &v[..])
            .unwrap_or(&[]);
        let known_vec = known_vecs
            .iter()
            .filter(|entry| entry.id == (id as usize))
            .next();
        let known_vec = match known_vec {
            None => return Err(SnapshotError::UnexpectedAppendVec),
            Some(v) => v,
        };

        let entries = AppendVec::new_from_file(path, known_vec.accounts_current_len)?;
        Ok(OwnedAppendVecAccountsIterBuilder {
            entries,
            iterator_builder: |entries| AppendVecAccountsIter::new(entries),
        }
        .build())
    }
}

fn parse_append_vec_name(name: &OsString) -> Option<(u64, u64)> {
    let name = name.to_str()?;
    let mut parts = name.splitn(2, '.');
    let slot = u64::from_str(parts.next().unwrap_or(""));
    let id = u64::from_str(parts.next().unwrap_or(""));
    match (slot, id) {
        (Ok(slot), Ok(version)) => Some((slot, version)),
        _ => None,
    }
}

#[self_referencing]
pub struct OwnedAppendVecAccountsIter {
    entries: AppendVec,
    #[borrows(entries)]
    #[not_covariant]
    pub(crate) iterator: AppendVecAccountsIter<'this>,
}

pub struct UnpackedSnapshotLoaderIter<'a, T>
    where T: Iterator<Item = Result<OwnedAppendVecAccountsIter, SnapshotError>> + 'a
{
    loader: &'a UnpackedSnapshotLoader,
    streams: T,
    iterator: Option<OwnedAppendVecAccountsIter>
}

impl<'a, T> UnpackedSnapshotLoaderIter<'a, T>
    where T: Iterator<Item = Result<OwnedAppendVecAccountsIter, SnapshotError>> + 'a
{
    fn new(loader: &'a UnpackedSnapshotLoader, mut streams: T) -> Result<Self, SnapshotError> {
        let iterator = streams.next().map_or(Ok(None), |v| v.map(Some))?;
        Ok(UnpackedSnapshotLoaderIter { loader, streams, iterator })
    }
}

impl<'a, T> Iterator for UnpackedSnapshotLoaderIter<'a, T>
    where T: Iterator<Item = Result<OwnedAppendVecAccountsIter, SnapshotError>> + 'a
{
    type Item = Result<StoredAccountMeta<'a>, SnapshotError>;

    fn next(&mut self) -> Option<Self::Item> {
        // Get next item of current stream.
        let mut stream = self.iterator?;
        if let Some(item) = stream.with_iterator_mut(|v| v.next()) {
            return Some(Ok(item))
        }
        // End of stream reached.
        self.iterator = None;
        // Get next iterator.
        match self.streams.next() {
            None => None, // Done
            Some(Err(e)) => return Some(Err(e)), // Trip on error
            Some(Ok(mut v)) => {
                // Start next iterator.
                let item = v.with_iterator_mut(|v| v.next().map(|x| Ok(x)));
                self.iterator = Some(v);
                item
            }
        }
    }
}
