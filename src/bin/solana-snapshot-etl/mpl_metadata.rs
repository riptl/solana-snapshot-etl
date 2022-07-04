use borsh::BorshDeserialize;
use solana_program::pubkey::Pubkey;

solana_program::declare_id!("metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s");

#[derive(BorshDeserialize)]
pub enum AccountKey {
    Uninitialized,
    EditionV1,
    MasterEditionV1,
    ReservationListV1,
    MetadataV1,
    ReservationListV2,
    MasterEditionV2,
    EditionMarker,
    UseAuthorityRecord,
    CollectionAuthorityRecord,
}

#[derive(BorshDeserialize)]
pub struct Metadata {
    pub update_authority: Pubkey,
    pub mint: Pubkey,
    pub data: Data,
    pub primary_sale_happened: bool,
    pub is_mutable: bool,
}

#[derive(BorshDeserialize)]
pub struct MetadataExt {
    pub edition_nonce: Option<u8>,
}

#[derive(BorshDeserialize)]
pub struct MetadataExtV1_2 {
    pub token_standard: Option<u8>,
    pub collection: Option<Collection>,
    pub uses: Option<Uses>,
}

#[derive(BorshDeserialize)]
pub struct Data {
    pub name: String,
    pub symbol: String,
    pub uri: String,
    pub seller_fee_basis_points: u16,
    pub creators: Option<Vec<Creator>>,
}

#[derive(BorshDeserialize)]
pub struct DataV2 {
    pub name: String,
    pub symbol: String,
    pub uri: String,
    pub seller_fee_basis_points: u16,
    pub creators: Option<Vec<Creator>>,
    pub collection: Option<Collection>,
    pub uses: Option<Uses>,
}

#[derive(BorshDeserialize)]
pub struct Creator {
    pub address: Pubkey,
    pub verified: bool,
    pub share: u8,
}

#[derive(BorshDeserialize)]
pub struct Collection {
    pub verified: bool,
    pub key: Pubkey,
}

#[derive(BorshDeserialize)]
pub struct Uses {
    pub use_method: u8,
    pub remaining: u64,
    pub total: u64,
}

#[derive(BorshDeserialize)]
pub enum CollectionDetails {
    V1 { size: u64 },
}
