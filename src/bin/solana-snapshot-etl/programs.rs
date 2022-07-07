use bincode::Options;
use solana_program::bpf_loader_upgradeable::UpgradeableLoaderState;
use solana_program::pubkey::Pubkey;
use solana_program::{bpf_loader, bpf_loader_deprecated, bpf_loader_upgradeable};
use solana_snapshot_etl::append_vec::{AppendVec, StoredAccountMeta};
use solana_snapshot_etl::append_vec_iter;
use solana_snapshot_etl::parallel::{AppendVecConsumer, GenericResult};
use std::io::Write;
use std::rc::Rc;
use tar::{Builder, Header};

pub(crate) struct ProgramDumper {
    builder: Builder<Box<dyn Write>>,
}

impl AppendVecConsumer for ProgramDumper {
    fn on_append_vec(&mut self, append_vec: AppendVec) -> GenericResult<()> {
        for account in append_vec_iter(Rc::new(append_vec)) {
            self.insert_account(&account.access().unwrap())?;
        }
        Ok(())
    }
}

impl ProgramDumper {
    pub(crate) fn new(writer: Box<dyn Write>) -> Self {
        Self {
            builder: Builder::new(writer),
        }
    }

    pub(crate) fn insert_account(&mut self, account: &StoredAccountMeta) -> GenericResult<()> {
        if bpf_loader_deprecated::check_id(&account.account_meta.owner)
            || bpf_loader::check_id(&account.account_meta.owner)
        {
            if account.account_meta.executable {
                self.write_executable(&account.meta.pubkey, account.data)?;
            }
        } else if bpf_loader_upgradeable::check_id(&account.account_meta.owner) {
            let header: UpgradeableLoaderState = bincode::options()
                .with_fixint_encoding()
                .allow_trailing_bytes()
                .deserialize(account.data)?;
            match header {
                UpgradeableLoaderState::ProgramData { .. } => {
                    self.write_executable(&account.meta.pubkey, &account.data[45..])?;
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn write_executable(&mut self, address: &Pubkey, data: &[u8]) -> GenericResult<()> {
        let mut header = Header::new_ustar();
        header.set_path(format!("{}.so", address))?;
        header.set_size(data.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        self.builder.append(&header, data)?;
        Ok(())
    }
}
