use anyhow::{Context, Result};
use std::path::Path;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkType {
    Reflink,
    HardLink,
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

    match reflink::reflink(master, &temp) {
        Ok(_) => {
            std::fs::rename(&temp, target).with_context(|| "replace target with reflink")?;
            Ok(Some(LinkType::Reflink))
        }
        Err(_) => {
            if temp.exists() {
                let _ = std::fs::remove_file(&temp);
            }
            std::fs::hard_link(master, &temp).with_context(|| "create hard link")?;
            std::fs::rename(&temp, target).with_context(|| "replace target with hard link")?;
            Ok(Some(LinkType::HardLink))
        }
    }
}
