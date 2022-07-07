// TODO add multi-threading

use indicatif::{ProgressBar, ProgressStyle};
use solana_geyser_plugin_interface::geyser_plugin_interface::{
    GeyserPlugin, ReplicaAccountInfoV2, ReplicaAccountInfoVersions,
};
use solana_snapshot_etl::append_vec::{AppendVec, StoredAccountMeta};
use solana_snapshot_etl::append_vec_iter;
use solana_snapshot_etl::parallel::{AppendVecConsumer, GenericResult};
use std::error::Error;
use std::rc::Rc;

pub(crate) struct GeyserDumper {
    accounts_spinner: ProgressBar,
    plugin: Box<dyn GeyserPlugin>,
    accounts_count: u64,
}

impl AppendVecConsumer for GeyserDumper {
    fn on_append_vec(&mut self, append_vec: AppendVec) -> GenericResult<()> {
        for account in append_vec_iter(Rc::new(append_vec)) {
            let account = account.access().unwrap();
            self.dump_account(account)?;
        }
        Ok(())
    }
}

impl GeyserDumper {
    pub(crate) fn new(plugin: Box<dyn GeyserPlugin>) -> Self {
        // TODO dedup spinner definitions
        let spinner_style = ProgressStyle::with_template(
            "{prefix:>10.bold.dim} {spinner} rate={per_sec}/s total={human_pos}",
        )
        .unwrap();
        let accounts_spinner = ProgressBar::new_spinner()
            .with_style(spinner_style)
            .with_prefix("accs");

        Self {
            accounts_spinner,
            plugin,
            accounts_count: 0,
        }
    }

    pub(crate) fn dump_account(
        &mut self,
        account: StoredAccountMeta,
    ) -> Result<(), Box<dyn Error>> {
        let slot = 0u64; // TODO fix slot number
        self.plugin.update_account(
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
        self.accounts_count += 1;
        if self.accounts_count % 1024 == 0 {
            self.accounts_spinner.set_position(self.accounts_count);
        }
        Ok(())
    }
}

impl Drop for GeyserDumper {
    fn drop(&mut self) {
        self.accounts_spinner.finish();
    }
}
