use anyhow::{Context, Result};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkType {
    Reflink,
    HardLink,
}

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

pub fn replace_with_link(master: &Path, target: &Path) -> Result<Option<LinkType>> {
    if master == target {
        return Ok(None);
    }

    let mut temp = target.to_path_buf();
    temp.set_extension("imprint_tmp");
    if temp.exists() {
        std::fs::remove_file(&temp).with_context(|| "remove existing temp file")?;
    }

    let mut cleanup = TempCleanup::new(temp.clone());

    match reflink::reflink(master, &temp) {
        Ok(_) => {
            std::fs::rename(&temp, target).with_context(|| "replace target with reflink")?;
            cleanup.disarm();
            Ok(Some(LinkType::Reflink))
        }
        Err(_) => {
            if temp.exists() {
                let _ = std::fs::remove_file(&temp);
            }
            std::fs::hard_link(master, &temp).with_context(|| "create hard link")?;
            std::fs::rename(&temp, target).with_context(|| "replace target with hard link")?;
            cleanup.disarm();
            Ok(Some(LinkType::HardLink))
        }
    }
}
