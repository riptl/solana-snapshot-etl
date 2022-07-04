use crate::geyser::load_plugin;
use crate::sqlite::SqliteIndexer;
use clap::{ArgGroup, Parser};
use indicatif::{ProgressBar, ProgressBarIter, ProgressStyle};
use log::info;
use reqwest::blocking::Response;
use serde::Serialize;
use solana_geyser_plugin_interface::geyser_plugin_interface::{
    ReplicaAccountInfoV2, ReplicaAccountInfoVersions,
};
use solana_snapshot_etl::{
    ArchiveSnapshotLoader, ReadProgressTracking, SnapshotLoader, StoredAccountMetaHandle,
    UnpackedSnapshotLoader,
};
use std::fs::File;
use std::io::{IoSliceMut, Read};
use std::path::{Path, PathBuf};

mod geyser;
mod mpl_metadata;
mod sqlite;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
#[clap(group(
    ArgGroup::new("action")
        .required(true)
        .args(&["csv", "geyser", "sqlite-out"]),
))]
struct Args {
    #[clap(help = "Path to snapshot")]
    path: String,
    #[clap(long, action, help = "Write CSV to stdout")]
    csv: bool,
    #[clap(long, help = "Export to new SQLite3 DB at this path")]
    sqlite_out: Option<String>,
    #[clap(long, action, help = "Index token program data")]
    tokens: bool,
    #[clap(long, help = "Load Geyser plugin from given config file")]
    geyser: Option<String>,
}

fn main() {
    env_logger::init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"),
    );
    if let Err(e) = _main() {
        eprintln!("{}", e);
        std::process::exit(1);
    }
}

fn _main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let mut loader = SupportedLoader::new(&args.path, Box::new(LoadProgressTracking {}))?;
    if args.csv {
        info!("Dumping to CSV");
        let spinner_style = ProgressStyle::with_template(
            "{prefix:>10.bold.dim} {spinner} rate={per_sec}/s total={human_pos}",
        )
        .unwrap();
        let accounts_spinner = ProgressBar::new_spinner()
            .with_style(spinner_style)
            .with_prefix("accs");
        let mut accounts_count = 0u64;
        let mut writer = csv::Writer::from_writer(std::io::stdout());
        for account in loader.iter() {
            let account = account?;
            let account = account.access().unwrap();
            let record = CSVRecord {
                pubkey: account.meta.pubkey.to_string(),
                owner: account.account_meta.owner.to_string(),
                data_len: account.meta.data_len,
                lamports: account.account_meta.lamports,
            };
            if writer.serialize(record).is_err() {
                std::process::exit(1); // if stdout closes, silently exit
            }
            accounts_count += 1;
            if accounts_count % 1024 == 0 {
                accounts_spinner.set_position(accounts_count);
            }
        }
        accounts_spinner.finish();
        println!("Done!");
    }
    if let Some(geyser_config_path) = args.geyser {
        info!("Dumping to Geyser plugin: {}", &geyser_config_path);
        let mut plugin = unsafe { load_plugin(&geyser_config_path)? };
        assert!(
            plugin.account_data_notifications_enabled(),
            "Geyser plugin does not accept account data notifications"
        );
        // TODO dedup spinner definitions
        let spinner_style = ProgressStyle::with_template(
            "{prefix:>10.bold.dim} {spinner} rate={per_sec}/s total={human_pos}",
        )
        .unwrap();
        let accounts_spinner = ProgressBar::new_spinner()
            .with_style(spinner_style)
            .with_prefix("accs");
        let mut accounts_count = 0u64;
        for account in loader.iter() {
            let account = account?;
            let account = account.access().unwrap();
            let slot = 0u64; // TODO fix slot number
            plugin.update_account(
                ReplicaAccountInfoVersions::V0_0_2(&ReplicaAccountInfoV2 {
                    pubkey: account.meta.pubkey.as_ref(),
                    lamports: account.account_meta.lamports,
                    owner: account.account_meta.owner.as_ref(),
                    executable: account.account_meta.executable,
                    rent_epoch: account.account_meta.rent_epoch,
                    data: account.data,
                    write_version: account.meta.write_version,
                    txn_signature: None,
                }),
                slot,
                /* is_startup */ false,
            )?;
            accounts_count += 1;
            if accounts_count % 1024 == 0 {
                accounts_spinner.set_position(accounts_count);
            }
        }
        accounts_spinner.finish();
        println!("Done!");
    }
    if let Some(sqlite_out_path) = args.sqlite_out {
        info!("Dumping to SQLite3: {}", &sqlite_out_path);
        let db_path = PathBuf::from(sqlite_out_path);
        if db_path.exists() {
            return Err("Refusing to overwrite database that already exists".into());
        }

        let indexer = SqliteIndexer::new(db_path)?;
        let stats = indexer.insert_all(loader.iter())?;

        info!(
            "Done! Wrote {} token accounts out of {} total",
            stats.token_accounts_total, stats.accounts_total
        );
    }
    Ok(())
}

struct LoadProgressTracking {}

impl ReadProgressTracking for LoadProgressTracking {
    fn new_read_progress_tracker(
        &self,
        _: &Path,
        rd: Box<dyn Read>,
        file_len: u64,
    ) -> Box<dyn Read> {
        let progress_bar = ProgressBar::new(file_len).with_style(
            ProgressStyle::with_template(
                "{prefix:>10.bold.dim} {spinner:.green} [{bar:.cyan/blue}] {bytes}/{total_bytes} ({percent}%)",
            )
            .unwrap()
            .progress_chars("#>-"),
        );
        progress_bar.set_prefix("manifest");
        Box::new(LoadProgressTracker {
            rd: progress_bar.wrap_read(rd),
            progress_bar,
        })
    }
}

struct LoadProgressTracker {
    progress_bar: ProgressBar,
    rd: ProgressBarIter<Box<dyn Read>>,
}

impl Drop for LoadProgressTracker {
    fn drop(&mut self) {
        self.progress_bar.finish()
    }
}

impl Read for LoadProgressTracker {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.rd.read(buf)
    }

    fn read_vectored(&mut self, bufs: &mut [IoSliceMut<'_>]) -> std::io::Result<usize> {
        self.rd.read_vectored(bufs)
    }

    fn read_to_string(&mut self, buf: &mut String) -> std::io::Result<usize> {
        self.rd.read_to_string(buf)
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        self.rd.read_exact(buf)
    }
}

#[derive(Serialize)]
struct CSVRecord {
    pubkey: String,
    owner: String,
    data_len: u64,
    lamports: u64,
}

pub enum SupportedLoader {
    Unpacked(UnpackedSnapshotLoader),
    ArchiveFile(ArchiveSnapshotLoader<File>),
    ArchiveDownload(ArchiveSnapshotLoader<Response>),
}

impl SupportedLoader {
    fn new(
        source: &str,
        progress_tracking: Box<dyn ReadProgressTracking>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        if source.starts_with("http://") || source.starts_with("https://") {
            Self::new_download(source)
        } else {
            Self::new_file(source.as_ref(), progress_tracking).map_err(Into::into)
        }
    }

    fn new_download(url: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let resp = reqwest::blocking::get(url)?;
        let loader = ArchiveSnapshotLoader::from_reader(resp)?;
        info!("Streaming snapshot from HTTP");
        Ok(Self::ArchiveDownload(loader))
    }

    fn new_file(
        path: &Path,
        progress_tracking: Box<dyn ReadProgressTracking>,
    ) -> solana_snapshot_etl::Result<Self> {
        Ok(if path.is_dir() {
            info!("Reading unpacked snapshot");
            Self::Unpacked(UnpackedSnapshotLoader::open(path, progress_tracking)?)
        } else {
            info!("Reading snapshot archive");
            Self::ArchiveFile(ArchiveSnapshotLoader::open(path)?)
        })
    }
}

impl SnapshotLoader for SupportedLoader {
    fn iter(
        &mut self,
    ) -> Box<dyn Iterator<Item = solana_snapshot_etl::Result<StoredAccountMetaHandle>> + '_> {
        match self {
            SupportedLoader::Unpacked(loader) => Box::new(loader.iter()),
            SupportedLoader::ArchiveFile(loader) => Box::new(loader.iter()),
            SupportedLoader::ArchiveDownload(loader) => Box::new(loader.iter()),
        }
    }
}
