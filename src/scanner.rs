use anyhow::Result;
use crossbeam::channel::Sender;
use jwalk::WalkDir;
use std::path::{Path, PathBuf};

pub fn stream_scan(root: &Path, tx: Sender<Result<PathBuf, (PathBuf, String)>>) -> Result<()> {
    for entry in WalkDir::new(root).into_iter() {
        let entry = match entry {
            Ok(entry) => entry,
            Err(e) => {
                let _ = tx.send(Err((PathBuf::from(root), format!("Walk error: {e}"))));
                continue;
            }
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
        match entry.metadata() {
            Ok(_) => {
                let path = entry.path().to_path_buf();
                let _ = tx.send(Ok(path));
            }
            Err(e) => {
                let _ = tx.send(Err((
                    entry.path().to_path_buf(),
                    format!("Metadata error: {e}"),
                )));
            }
        };
    }
    Ok(())
}
