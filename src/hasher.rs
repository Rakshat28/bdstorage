use crate::types::Hash;
use anyhow::{Context, Result};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

const SPARSE_CHUNK: usize = 4 * 1024;
const SPARSE_TOTAL: u64 = 12 * 1024;
const FULL_BUF: usize = 128 * 1024;
const MMAP_THRESHOLD: u64 = 1024 * 1024;

pub fn sparse_hash(path: &Path, size: u64) -> Result<Hash> {
    if size <= SPARSE_TOTAL {
        return full_hash(path);
    }

    let mut file = File::open(path).with_context(|| format!("open file {:?}", path))?;
    let mut hasher = blake3::Hasher::new();

    let mut buffer = vec![0u8; SPARSE_CHUNK];

    read_at(&mut file, 0, &mut buffer)?;
    hasher.update(&buffer);

    let mid_target = (size / 2).saturating_sub((SPARSE_CHUNK / 2) as u64);
    let middle = adjust_offset_for_sparse(&file, mid_target, size);
    read_at(&mut file, middle, &mut buffer)?;
    hasher.update(&buffer);

    let end = size.saturating_sub(SPARSE_CHUNK as u64);
    read_at(&mut file, end, &mut buffer)?;
    hasher.update(&buffer);

    Ok(hasher.finalize().into())
}

pub fn full_hash(path: &Path) -> Result<Hash> {
    let mut file = File::open(path).with_context(|| format!("open file {:?}", path))?;
    let len = file
        .metadata()
        .with_context(|| format!("read metadata {:?}", path))?
        .len();

    if len == 0 {
        return Ok(blake3::hash(b"").into());
    }

    #[cfg(unix)]
    if len >= MMAP_THRESHOLD {
        // SAFETY: The file may be modified by another process while it is mapped.
        // bdstorage operates on user-owned files not expected to be concurrently
        // written. In the worst case a concurrent write produces a stale hash that
        // the next scan will correct. The mapped region is used as a read-only
        // &[u8] slice by BLAKE3, so no Rust memory-safety invariant is violated.
        let mmap = unsafe { memmap2::Mmap::map(&file) }
            .with_context(|| format!("mmap file {:?}", path))?;
        return Ok(blake3::hash(&mmap[..]).into());
    }

    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0u8; FULL_BUF];
    loop {
        let read = file.read(&mut buffer).with_context(|| "read file")?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finalize().into())
}

/// Always uses the 128 KB BufReader loop, regardless of file size.
/// Exposed for benchmarking so the mmap and buffered paths can be compared directly.
pub(crate) fn full_hash_buffered(path: &Path) -> Result<Hash> {
    let mut file = File::open(path).with_context(|| format!("open file {:?}", path))?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0u8; FULL_BUF];
    loop {
        let read = file.read(&mut buffer).with_context(|| "read file")?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finalize().into())
}

fn read_at(file: &mut File, offset: u64, buffer: &mut [u8]) -> Result<()> {
    file.seek(SeekFrom::Start(offset))
        .with_context(|| "seek file")?;
    file.read_exact(buffer)
        .with_context(|| "read sparse chunk")?;
    Ok(())
}

#[cfg(target_os = "linux")]
fn adjust_offset_for_sparse(file: &File, target: u64, _file_size: u64) -> u64 {
    use nix::ioctl_readwrite;
    use std::mem;
    use std::os::unix::io::AsRawFd;

    #[repr(C)]
    struct FiemapExtent {
        fe_logical: u64,
        fe_physical: u64,
        fe_length: u64,
        fe_flags: u32,
        fe_reserved: u32,
    }

    #[repr(C)]
    struct Fiemap {
        fm_start: u64,
        fm_length: u64,
        fm_flags: u32,
        fm_mapped_extents: u32,
        fm_extent_count: u32,
        fm_reserved: u32,
        fm_extents: [FiemapExtent; 32],
    }

    ioctl_readwrite!(fiemap, b'f', 11, Fiemap);

    let fd = file.as_raw_fd();
    let mut fiemap_data: Fiemap = unsafe { mem::zeroed() };
    fiemap_data.fm_start = target;
    fiemap_data.fm_length = u64::MAX;
    fiemap_data.fm_extent_count = 32;

    if unsafe { fiemap(fd, &mut fiemap_data).is_ok() } {
        let mapped = fiemap_data.fm_mapped_extents as usize;
        if mapped > 0 {
            for i in 0..mapped {
                let extent = &fiemap_data.fm_extents[i];
                let extent_start = extent.fe_logical;
                let extent_end = extent_start + extent.fe_length;

                if target >= extent_start && target < extent_end {
                    return target;
                }
                if extent_start > target {
                    return extent_start;
                }
            }
        }
    }

    target
}

#[cfg(not(target_os = "linux"))]
fn adjust_offset_for_sparse(_file: &File, target: u64, _file_size: u64) -> u64 {
    target
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn empty_file_produces_blake3_empty_hash() {
        let f = NamedTempFile::new().unwrap();
        let got = full_hash(f.path()).unwrap();
        let want: Hash = blake3::hash(b"").into();
        assert_eq!(got, want);
    }

    #[test]
    fn buffered_and_mmap_paths_agree() {
        // File size is above MMAP_THRESHOLD so the mmap branch is exercised on Unix.
        // On non-Unix the buffered path runs for both; the assertion still holds.
        let data = vec![0x5Au8; (MMAP_THRESHOLD + 4096) as usize];
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(&data).unwrap();
        f.flush().unwrap();

        let got = full_hash(f.path()).unwrap();
        let want: Hash = blake3::hash(&data).into();
        assert_eq!(got, want, "mmap and reference hash must match");
    }

    #[test]
    fn small_file_below_mmap_threshold_is_correct() {
        let data = b"hello bdstorage";
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(data).unwrap();
        f.flush().unwrap();

        let got = full_hash(f.path()).unwrap();
        let want: Hash = blake3::hash(data).into();
        assert_eq!(got, want);
    }
}
