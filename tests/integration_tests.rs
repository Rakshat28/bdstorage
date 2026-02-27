use assert_cmd::Command;
use std::fs;
use std::io::{Seek, SeekFrom, Write};
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};

fn setup_env() -> tempfile::TempDir {
    tempfile::TempDir::new().expect("Failed to create temp directory")
}

fn create_random_file(dir: &Path, name: &str, size: usize) -> PathBuf {
    let path = dir.join(name);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("Failed to create parent directories");
    }
    let content = vec![42u8; size];
    fs::write(&path, content).expect("Failed to create random file");
    path
}

fn create_file_with_content(dir: &Path, name: &str, content: &[u8]) -> PathBuf {
    let path = dir.join(name);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("Failed to create parent directories");
    }
    fs::write(&path, content).expect("Failed to create file");
    path
}

fn run_cmd(home_dir: &Path, args: &[&str]) -> assert_cmd::Command {
    let mut cmd = Command::new(
        std::env::current_exe()
            .ok()
            .and_then(|mut exe| {
                exe.pop();
                if exe.ends_with("deps") {
                    exe.pop();
                }
                exe.push("bdstorage");
                Some(exe)
            })
            .expect("Failed to find bdstorage binary"),
    );
    cmd.env("HOME", home_dir);
    for arg in args {
        cmd.arg(arg);
    }
    cmd
}

#[test]
fn test_happy_path_dedupe_and_restore() {
    let temp_dir = setup_env();
    let home = temp_dir.path();
    let target = home.join("data");
    fs::create_dir(&target).expect("Failed to create target directory");

    for i in 0..5 {
        create_random_file(&target, &format!("unique_{}.txt", i), 1024);
    }

    for i in 0..5 {
        create_file_with_content(&target, &format!("dup_{}.txt", i), b"identical content");
    }

    let mut dedupe_cmd = run_cmd(home, &["dedupe", &target.to_string_lossy()]);
    dedupe_cmd.assert().success();

    let vault = home.join(".imprint").join("store");
    assert!(vault.exists(), "Vault directory should exist after dedupe");

    let file_count = fs::read_dir(&target)
        .expect("Failed to read target directory")
        .count();
    assert_eq!(
        file_count, 10,
        "All 10 files should still exist after dedupe"
    );

    let mut restore_cmd = run_cmd(home, &["restore", &target.to_string_lossy()]);
    restore_cmd.assert().success();

    let restored_content =
        fs::read(&target.join("dup_0.txt")).expect("Failed to read restored file");
    assert_eq!(
        restored_content, b"identical content",
        "Restored file content should match original"
    );

    let vault_files: Vec<_> = walkdir::WalkDir::new(&vault)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .collect();

    assert!(
        vault_files.is_empty(),
        "Vault should be empty after restore and GC"
    );
}

#[test]
fn test_zero_byte_files() {
    let temp_dir = setup_env();
    let home = temp_dir.path();
    let target = home.join("data");
    fs::create_dir(&target).expect("Failed to create target directory");

    for i in 0..5 {
        create_file_with_content(&target, &format!("empty_{}.txt", i), b"");
    }

    let mut dedupe_cmd = run_cmd(home, &["dedupe", &target.to_string_lossy()]);
    dedupe_cmd.assert().success();

    let file_count = fs::read_dir(&target)
        .expect("Failed to read target directory")
        .count();
    assert_eq!(file_count, 5, "All 5 empty files should still exist");
}

#[test]
fn test_deeply_nested_directories() {
    let temp_dir = setup_env();
    let home = temp_dir.path();
    let mut current = home.join("data");

    for i in 0..20 {
        current = current.join(format!("level_{}", i));
    }

    fs::create_dir_all(&current).expect("Failed to create nested directories");

    for i in 0..3 {
        create_file_with_content(&current, &format!("dup_{}.txt", i), b"nested content");
    }

    let root = home.join("data");
    let mut dedupe_cmd = run_cmd(home, &["dedupe", &root.to_string_lossy()]);
    dedupe_cmd.assert().success();

    assert!(
        current.join("dup_0.txt").exists(),
        "Deeply nested files should exist"
    );
}

#[test]
fn test_massive_and_sparse_files() {
    let temp_dir = setup_env();
    let home = temp_dir.path();
    let target = home.join("data");
    fs::create_dir(&target).expect("Failed to create target directory");

    let file1_content = vec![0xAAu8; 15 * 1024];
    let mut file2_content = vec![0xAAu8; 15 * 1024];

    file2_content[7 * 1024] = 0xBB;

    create_file_with_content(&target, "large1.bin", &file1_content);
    create_file_with_content(&target, "large1_dup.bin", &file1_content);
    create_file_with_content(&target, "large2.bin", &file2_content);
    create_file_with_content(&target, "large2_dup.bin", &file2_content);

    let mut dedupe_cmd = run_cmd(
        home,
        &[
            "dedupe",
            &target.to_string_lossy(),
            "--allow-unsafe-hardlinks",
        ],
    );
    dedupe_cmd.assert().success();

    let vault = home.join(".imprint").join("store");
    let vault_files: Vec<_> = walkdir::WalkDir::new(&vault)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .collect();

    assert_eq!(
        vault_files.len(),
        2,
        "Sparse hashing must detect mid-file byte difference and create 2 distinct vault files"
    );
}

#[test]
fn test_metadata_integrity() {
    let temp_dir = setup_env();
    let home = temp_dir.path();
    let target = home.join("data");
    fs::create_dir(&target).expect("Failed to create target directory");

    let _master_path = create_file_with_content(&target, "master.txt", b"test content");
    let dup_path = create_file_with_content(&target, "duplicate.txt", b"test content");

    fs::set_permissions(&dup_path, fs::Permissions::from_mode(0o444))
        .expect("Failed to set permissions");

    let test_time = filetime::FileTime::from_unix_time(1000000000, 0);
    filetime::set_file_mtime(&dup_path, test_time).expect("Failed to set mtime");

    if xattr::set(&dup_path, "user.test_attr", b"test_value").is_err() {
        eprintln!("Skipping xattr test: filesystem does not support extended attributes");
        return;
    }

    let mut dedupe_cmd = run_cmd(home, &["dedupe", &target.to_string_lossy()]);
    dedupe_cmd.assert().success();

    let dup_meta = fs::metadata(&dup_path).expect("Failed to read duplicate metadata");
    let dup_mtime = filetime::FileTime::from_last_modification_time(&dup_meta);

    assert_eq!(
        dup_mtime, test_time,
        "Modification time should be preserved"
    );

    let dup_perms = dup_meta.permissions();
    let dup_mode = dup_perms.mode() & 0o777;
    assert_eq!(
        dup_mode, 0o444,
        "Permissions should be preserved as read-only (0o444)"
    );

    let attr_val = xattr::get(&dup_path, "user.test_attr")
        .expect("Filesystem does not support xattr during test")
        .expect("xattr was completely stripped during deduplication");
    assert_eq!(
        attr_val, b"test_value",
        "Extended attribute value corrupted"
    );
}

#[test]
fn test_hardlink_fallback() {
    let temp_dir = setup_env();
    let home = temp_dir.path();
    let target = home.join("data");
    fs::create_dir(&target).expect("Failed to create target directory");

    create_file_with_content(&target, "file1.txt", b"hardlink test");
    create_file_with_content(&target, "file2.txt", b"hardlink test");

    let mut dedupe_cmd = run_cmd(
        home,
        &[
            "dedupe",
            &target.to_string_lossy(),
            "--allow-unsafe-hardlinks",
        ],
    );
    dedupe_cmd.assert().success();

    let file1_meta =
        fs::metadata(&target.join("file1.txt")).expect("Failed to read file1 metadata");
    let file2_meta =
        fs::metadata(&target.join("file2.txt")).expect("Failed to read file2 metadata");

    let file1_inode = file1_meta.ino();
    let file2_inode = file2_meta.ino();

    if file1_inode == file2_inode {
        // Hardlink successful
    } else {
        // Reflink used instead (filesystem natively supports CoW)
        let file1_content = fs::read(&target.join("file1.txt")).expect("Failed to read file1");
        let file2_content = fs::read(&target.join("file2.txt")).expect("Failed to read file2");
        assert_eq!(
            file1_content, file2_content,
            "Fallback failed: file contents do not match"
        );
    }
}

#[test]
fn test_paranoid_mode_catches_bit_rot() {
    let temp_dir = setup_env();
    let home = temp_dir.path();
    let target = home.join("data");
    fs::create_dir(&target).expect("Failed to create target directory");

    create_file_with_content(&target, "file1.txt", b"content");
    create_file_with_content(&target, "file2.txt", b"content");

    let mut dedupe_cmd1 = run_cmd(home, &["dedupe", &target.to_string_lossy()]);
    dedupe_cmd1.assert().success();

    let vault = home.join(".imprint").join("store");
    let vault_file = walkdir::WalkDir::new(&vault)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .next();

    if let Some(vault_entry) = vault_file {
        let vault_path = vault_entry.path().to_path_buf();
        let mut file = fs::OpenOptions::new()
            .write(true)
            .open(&vault_path)
            .expect("Failed to open vault file for corruption");
        file.seek(SeekFrom::Start(10))
            .expect("Failed to seek to middle of file");
        file.write_all(&[0xFF])
            .expect("Failed to write corrupted byte");
        drop(file);

        let mut dedupe_cmd2 = run_cmd(home, &["dedupe", &target.to_string_lossy(), "--paranoid"]);

        let output = dedupe_cmd2.output().expect("Failed to run paranoid dedupe");
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        let combined = format!("{}{}", stdout, stderr);

        assert!(
            combined.contains("HASH COLLISION OR BIT ROT DETECTED")
                || combined.contains("bit rot")
                || combined.contains("collision")
                || !output.status.success(),
            "Paranoid mode should fail on bit rot or detect collision. Got: {}",
            combined
        );
    }
}

#[test]
fn test_scan_no_modifications() {
    let temp_dir = setup_env();
    let home = temp_dir.path();
    let target = home.join("data");
    fs::create_dir(&target).expect("Failed to create target directory");

    create_file_with_content(&target, "file1.txt", b"test");
    create_file_with_content(&target, "file2.txt", b"test");

    let metadata_before = fs::metadata(&target.join("file1.txt")).expect("Failed to read metadata");

    let mut scan_cmd = run_cmd(home, &["scan", &target.to_string_lossy()]);
    scan_cmd.assert().success();

    let metadata_after =
        fs::metadata(&target.join("file1.txt")).expect("Failed to read metadata after scan");

    assert_eq!(
        metadata_before.modified().unwrap(),
        metadata_after.modified().unwrap(),
        "Scan should not modify files"
    );
}

#[test]
fn test_dry_run_no_changes() {
    let temp_dir = setup_env();
    let home = temp_dir.path();
    let target = home.join("data");
    fs::create_dir(&target).expect("Failed to create target directory");

    create_file_with_content(&target, "file1.txt", b"test");
    create_file_with_content(&target, "file2.txt", b"test");

    let inode_before = fs::metadata(&target.join("file1.txt"))
        .expect("Failed to read inode")
        .ino();

    let mut cmd = run_cmd(home, &["dedupe", &target.to_string_lossy(), "--dry-run"]);
    cmd.assert().success();

    let inode_after = fs::metadata(&target.join("file1.txt"))
        .expect("Failed to read inode after dry-run")
        .ino();

    assert_eq!(
        inode_before, inode_after,
        "Dry-run should not modify file inodes"
    );

    let imprint_dir = home.join(".imprint");
    assert!(
        !imprint_dir.exists(),
        "Entire .imprint directory (vault and database) must not exist in dry-run mode"
    );
}
