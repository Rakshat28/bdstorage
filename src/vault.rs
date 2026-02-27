use crate::types::{Hash, hash_to_hex};
use anyhow::{Context, Result};
use std::fs::File;
use std::path::{Path, PathBuf};

struct TempCleanup {
    path: PathBuf,
    armed: bool,
}

impl TempCleanup {
    fn new(path: PathBuf) -> Self {
        Self { path, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for TempCleanup {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if self.path.exists() {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

pub fn vault_root() -> Result<PathBuf> {
    let home = std::env::var("HOME").with_context(|| "HOME not set")?;
    Ok(PathBuf::from(home).join(".imprint").join("store"))
}

pub fn shard_path(hash: &Hash) -> Result<PathBuf> {
    let hex = hash_to_hex(hash);
    let shard_a = &hex[0..2];
    let shard_b = &hex[2..4];
    let root = vault_root()?;
    Ok(root.join(shard_a).join(shard_b).join(hex))
}

pub fn ensure_in_vault(hash: &Hash, src: &Path) -> Result<PathBuf> {
    let dest = shard_path(hash)?;
    if dest.exists() {
        return Ok(dest);
    }
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create vault directory {:?}", parent))?;
    }

    let mut temp = dest.to_path_buf();
    temp.set_extension("imprint_tmp");
    if temp.exists() {
        std::fs::remove_file(&temp).with_context(|| "remove existing temp file")?;
    }

    let mut cleanup = TempCleanup::new(temp.clone());
    let mut used_copy = false;

    match std::fs::rename(src, &temp) {
        Ok(_) => {}
        Err(_) => {
            std::fs::copy(src, &temp).with_context(|| "copy into vault temp")?;
            used_copy = true;
            let file = File::open(&temp).with_context(|| "open vault temp for sync")?;
            file.sync_all().with_context(|| "sync vault temp")?;
        }
    }

    std::fs::rename(&temp, &dest).with_context(|| "finalize vault file")?;
    cleanup.disarm();

    if used_copy {
        std::fs::remove_file(src).with_context(|| "remove original after copy")?;
    }

    Ok(dest)
}

pub fn remove_from_vault(hash: &Hash) -> Result<()> {
    let dest = shard_path(hash)?;
    if dest.exists() {
        std::fs::remove_file(&dest).with_context(|| "remove file from vault")?;

        if let Some(shard_b) = dest.parent() {
            if std::fs::read_dir(shard_b)
                .map(|mut i| i.next().is_none())
                .unwrap_or(false)
            {
                let _ = std::fs::remove_dir(shard_b);
                if let Some(shard_a) = shard_b.parent() {
                    if std::fs::read_dir(shard_a)
                        .map(|mut i| i.next().is_none())
                        .unwrap_or(false)
                    {
                        let _ = std::fs::remove_dir(shard_a);
                    }
                }
            }
        }
    }
    Ok(())
}
