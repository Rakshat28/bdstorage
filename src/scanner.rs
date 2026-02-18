use anyhow::Result;
use crossbeam::channel::Sender;
use jwalk::WalkDir;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

#[allow(dead_code)]
pub fn group_by_size(root: &Path) -> Result<HashMap<u64, Vec<PathBuf>>> {
    let mut groups: HashMap<u64, Vec<PathBuf>> = HashMap::new();

    for entry in WalkDir::new(root).into_iter() {
        let entry = match entry {
            Ok(entry) => entry,
            Err(_) => continue,
        };
        if !entry.file_type().is_file() {
            continue;
        }
        if entry
            .file_name()
            .to_string_lossy()
            .ends_with(".imprint_tmp")
        {
            continue;
        }
        let metadata = match entry.metadata() {
            Ok(metadata) => metadata,
            Err(_) => continue,
        };
        let size = metadata.len();
        groups
            .entry(size)
            .or_default()
            .push(entry.path().to_path_buf());
    }

    Ok(groups)
}

pub fn stream_scan(root: &Path, tx: Sender<PathBuf>) -> Result<()> {
    for entry in WalkDir::new(root).into_iter() {
        let entry = match entry {
            Ok(entry) => entry,
            Err(_) => continue,
        };
        if !entry.file_type().is_file() {
            continue;
        }
        if entry
            .file_name()
            .to_string_lossy()
            .ends_with(".imprint_tmp")
        {
            continue;
        }
        let _metadata = match entry.metadata() {
            Ok(metadata) => metadata,
            Err(_) => continue,
        };
        let path = entry.path().to_path_buf();
        let _ = tx.send(path);
    }
    Ok(())
}
