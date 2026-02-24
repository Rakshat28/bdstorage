use anyhow::{Context, Result};
use std::ffi::OsString;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use filetime::FileTime;

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

pub fn replace_with_link(master: &Path, target: &Path, allow_unsafe_hardlinks: bool) -> Result<Option<LinkType>> {
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
            // Capture target metadata before replacement (only for successful reflink)
            let target_meta = std::fs::metadata(target).with_context(|| "read target metadata")?;
            let target_permissions = target_meta.permissions();
            let target_mtime = FileTime::from_last_modification_time(&target_meta);
            
            // Capture extended attributes
            let mut target_xattrs: Vec<(OsString, Vec<u8>)> = Vec::new();
            if let Ok(attrs) = xattr::list(target) {
                for attr_name in attrs {
                    if let Ok(Some(attr_value)) = xattr::get(target, &attr_name) {
                        target_xattrs.push((attr_name, attr_value));
                    }
                }
            }
            
            std::fs::rename(&temp, target).with_context(|| "replace target with reflink")?;
            cleanup.disarm();
            
            // Restore metadata (safe for reflinks - different inodes)
            apply_metadata(target, &target_permissions, target_mtime, &target_xattrs)?;
            
            Ok(Some(LinkType::Reflink))
        }
        Err(_) => {
            if temp.exists() {
                let _ = std::fs::remove_file(&temp);
            }
            
            if allow_unsafe_hardlinks {
                // User explicitly opted into unsafe hardlinks
                std::fs::hard_link(master, &temp).with_context(|| "create hard link")?;
                std::fs::rename(&temp, target).with_context(|| "replace target with hard link")?;
                cleanup.disarm();
                
                // DO NOT restore metadata for hardlinks - they share the same inode as the vault master
                // Restoring metadata would mutate the vault master and all other linked copies
                
                Ok(Some(LinkType::HardLink))
            } else {
                // Safety first: refuse to create hardlinks without explicit permission
                anyhow::bail!("reflink not supported on this filesystem and --allow-unsafe-hardlinks not specified")
            }
        }
    }
}

fn apply_metadata(
    path: &Path,
    permissions: &std::fs::Permissions,
    mtime: FileTime,
    xattrs: &[(OsString, Vec<u8>)],
) -> Result<()> {
    // Restore permissions
    std::fs::set_permissions(path, permissions.clone())
        .with_context(|| "restore file permissions")?;
    
    // Restore modification time
    filetime::set_file_mtime(path, mtime)
        .with_context(|| "restore file mtime")?;
    
    // Restore extended attributes (gracefully ignore unsupported errors)
    for (attr_name, attr_value) in xattrs {
        let _ = xattr::set(path, attr_name, attr_value);
    }
    
    Ok(())
}

pub fn compare_files(path1: &Path, path2: &Path) -> Result<bool> {
    const BUFFER_SIZE: usize = 128 * 1024;

    let file1 = File::open(path1).with_context(|| "open file for compare (path1)")?;
    let file2 = File::open(path2).with_context(|| "open file for compare (path2)")?;

    let mut reader1 = BufReader::with_capacity(BUFFER_SIZE, file1);
    let mut reader2 = BufReader::with_capacity(BUFFER_SIZE, file2);

    let mut buf1 = vec![0u8; BUFFER_SIZE];
    let mut buf2 = vec![0u8; BUFFER_SIZE];

    loop {
        let read1 = reader1.read(&mut buf1)?;
        let read2 = reader2.read(&mut buf2)?;

        if read1 != read2 {
            return Ok(false);
        }
        if read1 == 0 {
            return Ok(true);
        }
        if buf1[..read1] != buf2[..read2] {
            return Ok(false);
        }
    }
}

pub fn restore_file(target: &Path) -> Result<()> {
    // Capture original metadata before restoration
    let target_meta = std::fs::metadata(target).with_context(|| "read target metadata")?;
    let target_permissions = target_meta.permissions();
    let target_mtime = FileTime::from_last_modification_time(&target_meta);
    
    // Capture extended attributes
    let mut target_xattrs: Vec<(OsString, Vec<u8>)> = Vec::new();
    if let Ok(attrs) = xattr::list(target) {
        for attr_name in attrs {
            if let Ok(Some(attr_value)) = xattr::get(target, &attr_name) {
                target_xattrs.push((attr_name, attr_value));
            }
        }
    }

    let mut temp = target.to_path_buf();
    temp.set_extension("imprint_tmp");
    if temp.exists() {
        std::fs::remove_file(&temp).with_context(|| "remove existing temp file")?;
    }

    let mut cleanup = TempCleanup::new(temp.clone());

    // Manual copy strictly breaks reflinks/shared extents by forcing byte allocation
    {
        let mut src = File::open(target).with_context(|| "open target for read")?;
        let mut dst = File::create(&temp).with_context(|| "create temp file")?;
        std::io::copy(&mut src, &mut dst).with_context(|| "copy bytes to temp file")?;
    }

    std::fs::rename(&temp, target).with_context(|| "replace target with restored copy")?;
    cleanup.disarm();
    
    // Restore metadata
    apply_metadata(target, &target_permissions, target_mtime, &target_xattrs)?;
    
    Ok(())
}
