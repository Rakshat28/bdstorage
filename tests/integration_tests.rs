use assert_cmd::Command;
use std::fs;
use std::io::{Seek, SeekFrom, Write};
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
#[cfg(unix)]
use xattr;

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
            .map(|mut exe| {
                exe.pop();
                if exe.ends_with("deps") {
                    exe.pop();
                }
                #[cfg(windows)]
                exe.push("bdstorage.exe");
                #[cfg(not(windows))]
                exe.push("bdstorage");
                exe
            })
            .expect("Failed to find bdstorage binary"),
    );
    #[cfg(windows)]
    cmd.env("USERPROFILE", home_dir);
    #[cfg(not(windows))]
    cmd.env("HOME", home_dir);
    for arg in args {
        cmd.arg(arg);
    }
    cmd
}

fn get_inode(metadata: &fs::Metadata) -> u64 {
    #[cfg(unix)]
    return metadata.ino();
    #[cfg(windows)]
    {
        let _ = metadata;
        0
    }
}

#[cfg(unix)]
fn get_mode(metadata: &fs::Metadata) -> u32 {
    metadata.permissions().mode() & 0o777
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
        fs::read(target.join("dup_0.txt")).expect("Failed to read restored file");
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

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&dup_path, fs::Permissions::from_mode(0o444))
            .expect("Failed to set permissions");
    }

    let test_time = filetime::FileTime::from_unix_time(1000000000, 0);
    filetime::set_file_mtime(&dup_path, test_time).expect("Failed to set mtime");

    #[cfg(unix)]
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

    #[cfg(unix)]
    {
        let _dup_perms = dup_meta.permissions();
        let dup_mode = get_mode(&dup_meta);
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

    let file1_meta = fs::metadata(target.join("file1.txt")).expect("Failed to read file1 metadata");
    let file2_meta = fs::metadata(target.join("file2.txt")).expect("Failed to read file2 metadata");

    let file1_inode = get_inode(&file1_meta);
    let file2_inode = get_inode(&file2_meta);

    if file1_inode == file2_inode {
    } else {
        let file1_content = fs::read(target.join("file1.txt")).expect("Failed to read file1");
        let file2_content = fs::read(target.join("file2.txt")).expect("Failed to read file2");
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
        .find(|e| e.file_type().is_file());

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

    let metadata_before = fs::metadata(target.join("file1.txt")).expect("Failed to read metadata");

    let mut scan_cmd = run_cmd(home, &["scan", &target.to_string_lossy()]);
    scan_cmd.assert().success();

    let metadata_after =
        fs::metadata(target.join("file1.txt")).expect("Failed to read metadata after scan");

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

    let inode_before =
        get_inode(&fs::metadata(target.join("file1.txt")).expect("Failed to read inode"));

    let mut cmd = run_cmd(home, &["dedupe", &target.to_string_lossy(), "--dry-run"]);
    cmd.assert().success();

    let inode_after = get_inode(
        &fs::metadata(target.join("file1.txt")).expect("Failed to read inode after dry-run"),
    );

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

fn run_cmd_with_vault_dir(vault_dir: &Path, args: &[&str]) -> assert_cmd::Command {
    let mut cmd = Command::new(
        std::env::current_exe()
            .ok()
            .map(|mut exe| {
                exe.pop();
                if exe.ends_with("deps") {
                    exe.pop();
                }
                exe.push("bdstorage");
                exe
            })
            .expect("Failed to find bdstorage binary"),
    );
    cmd.env("HOME", "/nonexistent");
    cmd.arg("--vault-dir").arg(vault_dir);
    for arg in args {
        cmd.arg(arg);
    }
    cmd
}

#[test]
fn test_vault_dir_flag_dedupe_and_restore() {
    let data_tmp = setup_env();
    let vault_tmp = setup_env();

    let target = data_tmp.path().join("data");
    fs::create_dir(&target).expect("create target dir");

    for i in 0..3 {
        create_file_with_content(
            &target,
            &format!("dup_{}.txt", i),
            b"vault-dir test content",
        );
    }

    let mut dedupe_cmd = run_cmd_with_vault_dir(
        vault_tmp.path(),
        &[
            "dedupe",
            &target.to_string_lossy(),
            "--allow-unsafe-hardlinks",
        ],
    );
    dedupe_cmd.assert().success();

    assert!(
        vault_tmp.path().join("state.redb").exists(),
        "state.redb should be in custom vault dir"
    );
    assert!(
        vault_tmp.path().join("store").exists(),
        "store/ should be in custom vault dir"
    );

    let mut restore_cmd =
        run_cmd_with_vault_dir(vault_tmp.path(), &["restore", &target.to_string_lossy()]);
    restore_cmd.assert().success();

    let content = fs::read(target.join("dup_0.txt")).expect("read restored file");
    assert_eq!(content, b"vault-dir test content");
}

#[test]
fn test_bdstorage_vault_env_var() {
    let data_tmp = setup_env();
    let vault_tmp = setup_env();

    let target = data_tmp.path().join("data");
    fs::create_dir(&target).expect("create target dir");

    for i in 0..3 {
        create_file_with_content(&target, &format!("dup_{}.txt", i), b"env var test content");
    }

    let mut cmd = Command::new(
        std::env::current_exe()
            .ok()
            .map(|mut exe| {
                exe.pop();
                if exe.ends_with("deps") {
                    exe.pop();
                }
                exe.push("bdstorage");
                exe
            })
            .expect("Failed to find bdstorage binary"),
    );
    cmd.env("HOME", "/nonexistent");
    cmd.env("BDSTORAGE_VAULT", vault_tmp.path());
    cmd.args([
        "dedupe",
        &target.to_string_lossy(),
        "--allow-unsafe-hardlinks",
    ]);
    cmd.assert().success();

    assert!(
        vault_tmp.path().join("state.redb").exists(),
        "state.redb should be in BDSTORAGE_VAULT dir"
    );
    assert!(
        vault_tmp.path().join("store").exists(),
        "store/ should be in BDSTORAGE_VAULT dir"
    );
}
#[test]
fn test_json_output_acceptance() {
    let temp_dir = setup_env();
    let home = temp_dir.path();
    let target = home.join("data");
    fs::create_dir(&target).expect("Failed to create target directory");

    create_file_with_content(&target, "file1.txt", b"1234");
    create_file_with_content(&target, "file2.txt", b"1234");

    // Test dedupe JSON
    let mut cmd = run_cmd(
        home,
        &[
            "--output-format",
            "json",
            "dedupe",
            &target.to_string_lossy(),
            "--allow-unsafe-hardlinks",
        ],
    );
    let output = cmd.assert().success().get_output().stdout.clone();
    let json_str = String::from_utf8_lossy(&output);
    let json: serde_json::Value =
        serde_json::from_str(&json_str).expect("Failed to parse dedupe JSON output");

    assert_eq!(json["files_scanned"], 2);
    assert_eq!(json["duplicate_groups"], 1);
    assert_eq!(json["bytes_saved"], 4);
    assert_eq!(json["links_created"], 2);
    assert_eq!(json["vault_objects_added"], 1);

    let scan_target = home.join("scan_data");
    fs::create_dir(&scan_target).expect("Failed to create scan target directory");
    create_file_with_content(&scan_target, "scan1.txt", b"scan duplicate");
    create_file_with_content(&scan_target, "scan2.txt", b"scan duplicate");

    // Test scan JSON on files that have not already been deduplicated.
    let mut cmd_scan = run_cmd(
        home,
        &[
            "--output-format",
            "json",
            "scan",
            &scan_target.to_string_lossy(),
        ],
    );
    let output_scan = cmd_scan.assert().success().get_output().stdout.clone();
    let json_scan_str = String::from_utf8_lossy(&output_scan);
    let json_scan: serde_json::Value =
        serde_json::from_str(&json_scan_str).expect("Failed to parse scan JSON output");

    assert_eq!(json_scan["files_scanned"], 2);
    assert_eq!(json_scan["duplicate_groups"], 1);

    // Test dry-run JSON on a fresh set of duplicates
    let dry_run_target = home.join("dry_run_data");
    fs::create_dir(&dry_run_target).expect("Failed to create dry-run target directory");
    create_file_with_content(&dry_run_target, "dry1.txt", b"1234");
    create_file_with_content(&dry_run_target, "dry2.txt", b"1234");

    let mut cmd_dry = run_cmd(
        home,
        &[
            "--output-format",
            "json",
            "dedupe",
            &dry_run_target.to_string_lossy(),
            "--dry-run",
        ],
    );
    let output_dry = cmd_dry.assert().success().get_output().stdout.clone();
    let json_dry_str = String::from_utf8_lossy(&output_dry);
    let json_dry: serde_json::Value =
        serde_json::from_str(&json_dry_str).expect("Failed to parse dry-run JSON output");

    assert_eq!(json_dry["files_scanned"], 2);
    assert_eq!(json_dry["duplicate_groups"], 1);
    assert_eq!(json_dry["bytes_saved"], 4);
    assert_eq!(json_dry["links_created"], 2);
    assert_eq!(json_dry["vault_objects_added"], 0);
}
