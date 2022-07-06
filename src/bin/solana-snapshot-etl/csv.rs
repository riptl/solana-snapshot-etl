use indicatif::{ProgressBar, ProgressStyle};
use serde::Serialize;
use solana_snapshot_etl::append_vec::{AppendVec, StoredAccountMeta};
use solana_snapshot_etl::append_vec_iter;
use std::io::Stdout;
use std::rc::Rc;

pub(crate) struct CsvDumper {
    accounts_spinner: ProgressBar,
    writer: csv::Writer<Stdout>,
    accounts_count: u64,
}

#[derive(Serialize)]
struct Record {
    pubkey: String,
    owner: String,
    data_len: u64,
    lamports: u64,
}

impl CsvDumper {
    pub(crate) fn new() -> Self {
        let spinner_style = ProgressStyle::with_template(
            "{prefix:>10.bold.dim} {spinner} rate={per_sec}/s total={human_pos}",
        )
        .unwrap();
        let accounts_spinner = ProgressBar::new_spinner()
            .with_style(spinner_style)
            .with_prefix("accs");

        let writer = csv::Writer::from_writer(std::io::stdout());

        Self {
            accounts_spinner,
            writer,
            accounts_count: 0,
        }
    }

    pub(crate) fn dump_append_vec(&mut self, append_vec: AppendVec) {
        for account in append_vec_iter(Rc::new(append_vec)) {
            let account = account.access().unwrap();
            self.dump_account(account);
        }
    }

    pub(crate) fn dump_account(&mut self, account: StoredAccountMeta) {
        let record = Record {
            pubkey: account.meta.pubkey.to_string(),
            owner: account.account_meta.owner.to_string(),
            data_len: account.meta.data_len,
            lamports: account.account_meta.lamports,
        };
        if self.writer.serialize(record).is_err() {
            std::process::exit(1); // if stdout closes, silently exit
        }
        self.accounts_count += 1;
        if self.accounts_count % 1024 == 0 {
            self.accounts_spinner.set_position(self.accounts_count);
        }
    }
}

impl Drop for CsvDumper {
    fn drop(&mut self) {
        self.accounts_spinner.finish();
    }
}
