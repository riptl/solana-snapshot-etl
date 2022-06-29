use bincode::Options;
use indicatif::{ProgressBar, ProgressStyle};
use log::debug;
use serde::Deserialize;
use solana_runtime::accounts_db::BankHashInfo;
use solana_runtime::ancestors::AncestorsForSerialization;
use solana_runtime::append_vec::{AppendVec, AppendVecAccountsIter, StoredMetaWriteVersion};
use solana_runtime::blockhash_queue::BlockhashQueue;
use solana_runtime::epoch_stakes::EpochStakes;
use solana_runtime::rent_collector::RentCollector;
use solana_runtime::snapshot_utils::SNAPSHOT_STATUS_CACHE_FILENAME;
use solana_runtime::stakes::Stakes;
use solana_sdk::clock::{Epoch, UnixTimestamp};
use solana_sdk::deserialize_utils::default_on_eof;
use solana_sdk::epoch_schedule::EpochSchedule;
use solana_sdk::fee_calculator::{FeeCalculator, FeeRateGovernor};
use solana_sdk::hard_forks::HardForks;
use solana_sdk::hash::Hash;
use solana_sdk::inflation::Inflation;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::slot_history::Slot;
use solana_sdk::stake::state::Delegation;
use std::collections::{HashMap, HashSet};
use std::ffi::OsString;
use std::fs::OpenOptions;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Instant;
use thiserror::Error;

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
        let versioned_bank: DeserializableVersionedBank = bincode::options()
            .with_limit(MAX_STREAM_SIZE)
            .with_fixint_encoding()
            .allow_trailing_bytes()
            .deserialize_from(&mut snapshot_file)?;
        drop(versioned_bank);
        let versioned_bank_post_time = Instant::now();

        let accounts_db_fields: AccountsDbFields<SerializableAccountStorageEntry> =
            bincode::options()
                .with_limit(MAX_STREAM_SIZE)
                .with_fixint_encoding()
                .allow_trailing_bytes()
                .deserialize_from(&mut snapshot_file)?;
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

    pub fn foo(&self) -> Result<(), SnapshotError> {
        let accounts_dir = self.root.join("accounts");
        let accounts_files = accounts_dir
            .read_dir()?
            .filter_map(|f| f.ok())
            .filter_map(|f| {
                let name = f.file_name();
                parse_append_vec_name(&f.file_name()).map(move |parsed| (parsed, name))
            });
        for ((slot, version), name) in accounts_files {
            self.stream_log(slot, version, &accounts_dir.join(name))?;
        }
        Ok(())
    }

    pub fn stream_log(&self, slot: u64, id: u64, path: &Path) -> Result<(), SnapshotError> {
        let known_vecs = self
            .accounts_db_fields
            .0
            .get(&slot)
            .map(|v| &v[..])
            .unwrap_or(&[]);
        println!("slot={} id={} known_vecs={:?}", slot, id, known_vecs);
        let known_vec = known_vecs.iter().filter(|entry| entry.id == (id as usize)).next();
        let known_vec = match known_vec {
            None => return Ok(()),
            Some(v) => v,
        };

        // inefficient
        let (entries, num_accounts) = AppendVec::new_from_file(path, known_vec.accounts_current_len)?;
        let iter = AppendVecAccountsIter::new(&entries);
        for entry in iter {
            println!("  {}", entry.meta.pubkey);
        }
        Ok(())
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

const MAX_STREAM_SIZE: u64 = 32 * 1024 * 1024 * 1024;

#[derive(Default, PartialEq, Eq, Debug, Deserialize)]
struct UnusedAccounts {
    unused1: HashSet<Pubkey>,
    unused2: HashSet<Pubkey>,
    unused3: HashMap<Pubkey, u64>,
}

#[derive(Deserialize)]
#[allow(dead_code)]
struct DeserializableVersionedBank {
    blockhash_queue: BlockhashQueue,
    ancestors: AncestorsForSerialization,
    hash: Hash,
    parent_hash: Hash,
    parent_slot: Slot,
    hard_forks: HardForks,
    transaction_count: u64,
    tick_height: u64,
    signature_count: u64,
    capitalization: u64,
    max_tick_height: u64,
    hashes_per_tick: Option<u64>,
    ticks_per_slot: u64,
    ns_per_slot: u128,
    genesis_creation_time: UnixTimestamp,
    slots_per_year: f64,
    accounts_data_len: u64,
    slot: Slot,
    epoch: Epoch,
    block_height: u64,
    collector_id: Pubkey,
    collector_fees: u64,
    fee_calculator: FeeCalculator,
    fee_rate_governor: FeeRateGovernor,
    collected_rent: u64,
    rent_collector: RentCollector,
    epoch_schedule: EpochSchedule,
    inflation: Inflation,
    stakes: Stakes<Delegation>,
    #[allow(dead_code)]
    unused_accounts: UnusedAccounts,
    epoch_stakes: HashMap<Epoch, EpochStakes>,
    is_delta: bool,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
struct AccountsDbFields<T>(
    HashMap<Slot, Vec<T>>,
    StoredMetaWriteVersion,
    Slot,
    BankHashInfo,
    /// all slots that were roots within the last epoch
    #[serde(deserialize_with = "default_on_eof")]
    Vec<Slot>,
    /// slots that were roots within the last epoch for which we care about the hash value
    #[serde(deserialize_with = "default_on_eof")]
    Vec<(Slot, Hash)>,
);

pub type SerializedAppendVecId = usize;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Deserialize)]
pub struct SerializableAccountStorageEntry {
    id: SerializedAppendVecId,
    accounts_current_len: usize,
}
