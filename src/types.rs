use serde::{Deserialize, Serialize};

pub type Hash = [u8; 32];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileMetadata {
    pub size: u64,
    pub modified: u64,
    pub hash: Hash,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessingError {
    pub path: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct JsonReport {
    pub files_scanned: usize,
    pub duplicate_groups: usize,
    pub bytes_saved: u64,
    pub vault_objects_added: usize,
    pub links_created: usize,
    pub errors: Vec<ProcessingError>,
}

pub fn hash_to_hex(hash: &Hash) -> String {
    blake3::Hash::from_bytes(*hash).to_hex().to_string()
}
