use clap::Parser;
use indicatif::{ProgressBar, ProgressBarIter, ProgressStyle};
use serde::Serialize;
use solana_snapshot_etl::{ReadProgressTracking, UnpackedSnapshotLoader};
use std::io::{IoSliceMut, Read};
use std::path::Path;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    path: String,
    #[clap(long, action)]
    csv: bool,
}

fn main() {
    env_logger::init();
    if let Err(e) = _main() {
        eprintln!("{}", e);
        std::process::exit(1);
    }
}

fn _main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let loader =
        UnpackedSnapshotLoader::open_with_progress(&args.path, Box::new(LoadProgressTracking {}))?;
    if args.csv {
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
        }
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
                "{spinner:.green} [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({percent}%)",
            )
            .unwrap()
            .progress_chars("#>-"),
        );
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
