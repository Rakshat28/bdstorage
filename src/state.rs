use crate::types::{FileMetadata, Hash};
use anyhow::{Context, Result};
use redb::{Database, TableDefinition};
use std::path::{Path, PathBuf};

const FILE_INDEX: TableDefinition<&[u8], &[u8]> = TableDefinition::new("file_index");
const CAS_INDEX: TableDefinition<&[u8], &[u8]> = TableDefinition::new("cas_index");
const VAULTED_INODES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("vaulted_inodes");

#[derive(Clone)]
pub struct State {
    db: std::sync::Arc<Database>,
}

impl State {
    pub fn open_default() -> Result<Self> {
        let db_path = default_db_path()?;
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create state directory {:?}", parent))?;
        }
        let db = Database::create(&db_path).with_context(|| "open redb database")?;
        let txn = db.begin_write().with_context(|| "begin write transaction")?;
        {
            let _ = txn.open_table(FILE_INDEX)?;
            let _ = txn.open_table(CAS_INDEX)?;
            let _ = txn.open_table(VAULTED_INODES)?;
        }
        txn.commit().with_context(|| "commit table initialization")?;
        Ok(Self {
            db: std::sync::Arc::new(db),
        })
    }

    pub fn upsert_file(&self, path: &Path, metadata: &FileMetadata) -> Result<()> {
        let key = path.to_string_lossy().as_bytes().to_vec();
        let value = bincode::serialize(metadata).with_context(|| "serialize file metadata")?;
        let txn = self.db.begin_write().with_context(|| "begin write transaction")?;
        {
            let mut table = txn.open_table(FILE_INDEX)?;
            table.insert(key.as_slice(), value.as_slice())?;
        }
        txn.commit().with_context(|| "commit file index write")?;
        Ok(())
    }

    pub fn set_cas_refcount(&self, hash: &Hash, count: u64) -> Result<()> {
        let key = hash.to_vec();
        let value = count.to_le_bytes().to_vec();
        let txn = self.db.begin_write().with_context(|| "begin write transaction")?;
        {
            let mut table = txn.open_table(CAS_INDEX)?;
            table.insert(key.as_slice(), value.as_slice())?;
        }
        txn.commit().with_context(|| "commit cas index write")?;
        Ok(())
    }

    pub fn is_inode_vaulted(&self, inode: u64) -> Result<bool> {
        let key = inode.to_le_bytes();
        let txn = self.db.begin_read().with_context(|| "begin read transaction")?;
        let table = match txn.open_table(VAULTED_INODES) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(false),
            Err(err) => return Err(err.into()),
        };
        Ok(table.get(key.as_slice())?.is_some())
    }

    pub fn mark_inode_vaulted(&self, inode: u64) -> Result<()> {
        let key = inode.to_le_bytes();
        let value = 1u8;
        let txn = self.db.begin_write().with_context(|| "begin write transaction")?;
        {
            let mut table = txn.open_table(VAULTED_INODES)?;
            table.insert(key.as_slice(), std::slice::from_ref(&value))?;
        }
        txn.commit().with_context(|| "commit vaulted inode write")?;
        Ok(())
    }

    pub fn get_file_metadata(&self, path: &Path) -> Result<Option<FileMetadata>> {
        let key = path.to_string_lossy().as_bytes().to_vec();
        let txn = self.db.begin_read().with_context(|| "begin read transaction")?;
        let table = match txn.open_table(FILE_INDEX) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(err) => return Err(err.into()),
        };
        if let Some(access) = table.get(key.as_slice())? {
            let metadata: FileMetadata = bincode::deserialize(access.value())
                .with_context(|| "deserialize file metadata")?;
            return Ok(Some(metadata));
        }
        Ok(None)
    }

    pub fn remove_file_from_index(&self, path: &Path) -> Result<()> {
        let key = path.to_string_lossy().as_bytes().to_vec();
        let txn = self.db.begin_write().with_context(|| "begin write transaction")?;
        {
            let mut table = txn.open_table(FILE_INDEX)?;
            table.remove(key.as_slice())?;
        }
        txn.commit().with_context(|| "commit file index removal")?;
        Ok(())
    }

    pub fn unmark_inode_vaulted(&self, inode: u64) -> Result<()> {
        let key = inode.to_le_bytes();
        let txn = self.db.begin_write().with_context(|| "begin write transaction")?;
        {
            let mut table = txn.open_table(VAULTED_INODES)?;
            table.remove(key.as_slice())?;
        }
        txn.commit().with_context(|| "commit unmark vaulted inode")?;
        Ok(())
    }

    pub fn get_cas_refcount(&self, hash: &Hash) -> Result<u64> {
        let key = hash.to_vec();
        let txn = self.db.begin_read().with_context(|| "begin read transaction")?;
        let table = match txn.open_table(CAS_INDEX) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(0),
            Err(err) => return Err(err.into()),
        };
        if let Some(access) = table.get(key.as_slice())? {
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(access.value());
            return Ok(u64::from_le_bytes(bytes));
        }
        Ok(0)
    }

    pub fn remove_cas_refcount(&self, hash: &Hash) -> Result<()> {
        let key = hash.to_vec();
        let txn = self.db.begin_write().with_context(|| "begin write transaction")?;
        {
            let mut table = txn.open_table(CAS_INDEX)?;
            table.remove(key.as_slice())?;
        }
        txn.commit().with_context(|| "commit cas index removal")?;
        Ok(())
    }
}

pub fn default_db_path() -> Result<PathBuf> {
    let home = std::env::var("HOME").with_context(|| "HOME not set")?;
    Ok(PathBuf::from(home).join(".imprint").join("state.redb"))
}
