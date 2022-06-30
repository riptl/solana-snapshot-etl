use clap::{ArgGroup, Parser};
use indicatif::{ProgressBar, ProgressBarIter, ProgressStyle};
use log::error;
use rusqlite::params;
use serde::Serialize;
use solana_sdk::program_pack::Pack;
use solana_snapshot_etl::{ReadProgressTracking, UnpackedSnapshotLoader};
use std::io::{IoSliceMut, Read};
use std::path::{Path, PathBuf};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
#[clap(group(
    ArgGroup::new("action")
        .required(true)
        .args(&["csv", "sqlite-out"]),
))]
struct Args {
    #[clap(help = "Path to snapshot")]
    path: String,
    #[clap(long, action, help = "Write CSV to stdout")]
    csv: bool,
    #[clap(long, help = "Export to new SQLite3 DB at this path")]
    sqlite_out: String,
    #[clap(long, help = "Index token program data")]
    tokens: bool,
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
    if !args.sqlite_out.is_empty() {
        let db_path = PathBuf::from(args.sqlite_out);
        assert!(
            !db_path.exists(),
            "Refusing to overwrite database that already exists"
        );

        // Create temporary DB file, which gets promoted on success.
        let temp_file_name = format!("_{}.tmp", db_path.file_name().unwrap().to_string_lossy());
        let temp_db_path = db_path.with_file_name(&temp_file_name);
        let _ = std::fs::remove_file(&temp_db_path);
        let mut temp_file_guard = TempFileGuard::new(temp_db_path.clone());

        // Open database.
        let db = rusqlite::Connection::open(&temp_db_path)?;
        db.pragma_update(None, "synchronous", false)?;
        db.pragma_update(None, "journal_mode", "off")?;
        db.execute(
            "\
CREATE TABLE token_mint (
    pubkey BLOB(32) NOT NULL PRIMARY KEY,
    mint_authority BLOB(32) NULL,
    supply INTEGER(8) NOT NULL,
    decimals INTEGER(2) NOT NULL,
    is_initialized BOOL NOT NULL,
    freeze_authority BLOB(32) NULL
);",
            [],
        )?;
        db.execute(
            "\
CREATE TABLE token_account (
    pubkey BLOB(32) NOT NULL PRIMARY KEY,
    mint BLOB(32) NOT NULL,
    owner BLOB(32) NOT NULL,
    amount INTEGER(8) NOT NULL,
    delegate BLOB(32),
    state INTEGER(1) NOT NULL,
    is_native INTEGER(8),
    delegated_amount INTEGER(8) NOT NULL,
    close_authority BLOB(32)
);",
            [],
        )?;
        db.execute(
            "\
CREATE TABLE token_multisig (
    pubkey BLOB(32) NOT NULL,
    signer BLOB(32) NOT NULL,
    m INTEGER(2) NOT NULL,
    n INTEGER(2) NOT NULL,
    PRIMARY KEY (pubkey, signer)
);",
            [],
        )?;

        let mut token_mint_insert = db.prepare("\
INSERT OR REPLACE INTO token_mint (pubkey, mint_authority, supply, decimals, is_initialized, freeze_authority)
    VALUES (?, ?, ?, ?, ?, ?);
")?;
        let mut token_account_insert = db.prepare("\
INSERT OR REPLACE INTO token_account (pubkey, mint, owner, amount, delegate, state, is_native, delegated_amount, close_authority)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
")?;
        let mut token_multisig_insert = db.prepare(
            "\
INSERT OR REPLACE INTO token_multisig (pubkey, signer, m, n)
    VALUES (?, ?, ?, ?);
",
        )?;
        for account in loader.iter() {
            let account = account?;
            let account = account.access().unwrap();
            if account.account_meta.owner == spl_token::id() {
                match account.meta.data_len as usize {
                    spl_token::state::Account::LEN => {
                        let token_account = spl_token::state::Account::unpack(account.data);
                        if let Ok(token_account) = token_account {
                            token_account_insert.insert(params![
                                account.meta.pubkey.as_ref(),
                                token_account.mint.as_ref(),
                                token_account.owner.as_ref(),
                                token_account.amount as i64,
                                Option::<[u8; 32]>::from(
                                    token_account.delegate.map(|key| key.to_bytes())
                                ),
                                token_account.state as u8,
                                Option::<u64>::from(token_account.is_native),
                                token_account.delegated_amount as i64,
                                Option::<[u8; 32]>::from(
                                    token_account.close_authority.map(|key| key.to_bytes())
                                ),
                            ])?;
                        }
                    }
                    spl_token::state::Mint::LEN => {
                        let token_mint = spl_token::state::Mint::unpack(account.data);
                        if let Ok(token_mint) = token_mint {
                            token_mint_insert.insert(params![
                                account.meta.pubkey.as_ref(),
                                Option::<[u8; 32]>::from(
                                    token_mint.mint_authority.map(|key| key.to_bytes()),
                                ),
                                token_mint.supply as i64,
                                token_mint.decimals,
                                token_mint.is_initialized,
                                Option::<[u8; 32]>::from(
                                    token_mint.freeze_authority.map(|key| key.to_bytes())
                                ),
                            ])?;
                        }
                    }
                    spl_token::state::Multisig::LEN => {
                        let token_multisig = spl_token::state::Multisig::unpack(account.data);
                        if let Ok(token_multisig) = token_multisig {
                            for signer in &token_multisig.signers[..token_multisig.n as usize] {
                                token_multisig_insert.insert(params![
                                    account.meta.pubkey.as_ref(),
                                    signer.as_ref(),
                                    token_multisig.m,
                                    token_multisig.n
                                ])?;
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
        drop(token_mint_insert);
        drop(token_account_insert);
        drop(token_multisig_insert);

        // Gracefully exit.
        drop(db); // close connection
        temp_file_guard.promote(db_path)?;
    }
    Ok(())
}

pub struct TempFileGuard {
    pub path: Option<PathBuf>,
}

impl TempFileGuard {
    fn new(path: PathBuf) -> Self {
        Self { path: Some(path) }
    }

    fn promote<P: AsRef<Path>>(&mut self, new_name: P) -> std::io::Result<()> {
        std::fs::rename(
            self.path.take().expect("cannot promote non-existent file"),
            new_name,
        )
    }
}

impl Drop for TempFileGuard {
    fn drop(&mut self) {
        if let Some(path) = &self.path {
            if let Err(e) = std::fs::remove_file(path) {
                error!("Failed to remove temp DB: {}", e);
            }
        }
    }
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
