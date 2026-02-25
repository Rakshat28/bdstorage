mod dedupe;
mod hasher;
mod scanner;
mod state;
mod types;
mod vault;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use colored::*;
use crossbeam::channel;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::collections::HashMap;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

use crate::types::{FileMetadata, Hash};

#[derive(Parser, Debug)]
#[command(
    name = "bdstorage",
    author,
    version,
    about = "bdstorage: A speed-first, local file deduplication engine.",
    long_about = "bdstorage uses a Tiered Hashing philosophy to minimize I/O overhead:\n\nSize Grouping: Eliminates unique file sizes immediately.\n\nSparse Hashing: Samples 12KB (start/middle/end) to identify candidates.\n\nFull BLAKE3 Hashing: Verifies matches with high-performance 128KB buffering.",
    help_template = "{before-help}{name} {version}\n{author-with-newline}{about-section}\n\nSTORAGE PATHS:\n  State DB: ~/.bdstorage/state.redb\n  CAS Vault: ~/.bdstorage/store\n\n{usage-heading} {usage}\n\nGLOBAL FLAGS:\n  -h, --help     Print help\n  -V, --version  Print version\n\nSUBCOMMAND FLAGS:\n  --paranoid                 Available on the dedupe subcommand. Forces a byte-for-byte\n                             verification before linking to guarantee 100% collision safety.\n\n  --allow-unsafe-hardlinks   Available on the dedupe subcommand. Allows hard link fallback\n                             when CoW reflinks are not supported. Hard links share the same\n                             inode, so all linked files will have identical metadata.\n\n  -n, --dry-run              Available on dedupe and restore subcommands. Simulates operations\n                             without modifying the filesystem or the database.\n\n{all-args}{after-help}"
)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Perform a read-only analysis of a directory.
    ///
    /// Scans the target path and identifies duplicate groups using tiered hashing
    /// without moving any files or modifying the filesystem.
    Scan {
        /// The directory to analyze
        path: PathBuf,
    },
    /// Deduplicate a directory by moving master copies to the vault.
    ///
    /// This is a write-mode operation. It moves the 'master' copy of a duplicate group
    /// into the internal vault and replaces all instances (including the original)
    /// with reflinks or hard links.
    Dedupe {
        /// The directory to deduplicate
        path: PathBuf,
        /// Perform byte-for-byte verification before linking to guarantee 100% collision safety.
        #[arg(long)]
        paranoid: bool,
        /// Simulate operations without modifying the filesystem or database.
        #[arg(long, short = 'n')]
        dry_run: bool,
        /// Allow unsafe hard links as fallback when CoW reflinks are not supported.
        /// Hard links share the same inode, so all linked files will have identical
        /// metadata and modifications affect all linked copies.
        #[arg(long, action = clap::ArgAction::SetTrue, default_value_t = false)]
        allow_unsafe_hardlinks: bool,
    },
    /// Restore deduplicated files to their original independent state.
    ///
    /// Breaks links and copies data back from the vault to the original location.
    ///    cargo run -- dedupe ~/Desktop/experiment/ --help    cargo run -- dedupe ~/Desktop/experiment/ --help If a vault file's reference count hits zero, it is pruned.
    Restore {
        /// The directory to restore
        path: PathBuf,
        /// Simulate operations without modifying the filesystem or database.
        #[arg(long, short = 'n')]
        dry_run: bool,
    },
}

fn main() {
    if let Err(err) = run() {
        eprintln!("{err:?}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let args = Args::parse();
    let state = state::State::open_default()?;

    match args.command {
        Commands::Scan { path } => {
            let groups = scan_pipeline(&path, &state)?;
            print_summary("scan", &groups);
        }
        Commands::Dedupe { path, paranoid, dry_run, allow_unsafe_hardlinks } => {
            let groups = scan_pipeline(&path, &state)?;
            dedupe_groups(&groups, &state, paranoid, dry_run, allow_unsafe_hardlinks)?;
            print_summary("dedupe", &groups);
        }
        Commands::Restore { path, dry_run } => {
            restore_pipeline(&path, &state, dry_run)?;
        }
    }

    Ok(())
}

fn scan_pipeline(path: &Path, state: &state::State) -> Result<HashMap<Hash, Vec<PathBuf>>> {
    // Setup UI: Create MultiProgress and progress bars
    let multi = MultiProgress::new();
    let scan_spinner = multi.add(ProgressBar::new_spinner());
    scan_spinner.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner} {msg}")
            .unwrap(),
    );
    scan_spinner.set_message("Scanning...");

    let hash_bar = multi.add(progress("Indexing/Hashing", 0));

    // Spawn scanner thread with channel
    let (scan_tx, scan_rx) = channel::unbounded();
    let path_clone = path.to_path_buf();
    let scanner_handle = std::thread::spawn(move || -> Result<()> {
        scanner::stream_scan(&path_clone, scan_tx)
    });

    // Create channel for hashing tasks
    let (hash_task_tx, hash_task_rx) = channel::unbounded::<PathBuf>();

    // Create channel for results
    let (result_tx, result_rx) = channel::unbounded::<(Hash, PathBuf)>();

    // Spawn worker threads for hashing
    let state_clone = state.clone();
    let num_workers = std::cmp::min(rayon::current_num_threads(), 8);
    let mut worker_handles = vec![];

    for _ in 0..num_workers {
        let rx = hash_task_rx.clone();
        let tx = result_tx.clone();
        let state_ref = state_clone.clone();
        let hash_bar_clone = hash_bar.clone();

        let handle = std::thread::spawn(move || {
            while let Ok(file_path) = rx.recv() {
                if let Ok(metadata) = std::fs::metadata(&file_path) {
                    let inode = metadata.ino();
                    if let Ok(is_vaulted) = state_ref.is_inode_vaulted(inode) {
                        if is_vaulted {
                            continue;
                        }
                    }

                    let size = metadata.len();
                    if let Ok(_) = hasher::sparse_hash(&file_path, size) {
                        // For now, always perform full hash for indexing
                        if let Ok(full_hash) = hasher::full_hash(&file_path) {
                            let modified = file_modified(&file_path).unwrap_or(0);
                            let file_metadata = FileMetadata {
                                size,
                                modified,
                                hash: full_hash,
                            };
                            let _ = state_ref.upsert_file(&file_path, &file_metadata);
                            let _ = tx.send((full_hash, file_path));
                            hash_bar_clone.inc(1);
                        }
                    }
                }
            }
        });

        worker_handles.push(handle);
    }

    // Coordinator loop: maintain size_map and send collision files to hashing
    let mut size_map: HashMap<u64, Vec<PathBuf>> = HashMap::new();

    loop {
        match scan_rx.recv() {
            Ok(file_path) => {
                scan_spinner.tick();

                if let Ok(metadata) = std::fs::metadata(&file_path) {
                    let size = metadata.len();
                    let entry = size_map.entry(size).or_default();
                    let len_before = entry.len();
                    entry.push(file_path.clone());

                    // Collision trigger: send files to hashing when we hit count 2 or more
                    if len_before == 1 {
                        // First collision: send both files (index 0 and 1)
                        if let Some(first_file) = entry.get(0).cloned() {
                            let _ = hash_task_tx.send(first_file);
                        }
                        let _ = hash_task_tx.send(file_path);
                        hash_bar.set_length(hash_bar.length().unwrap_or(0) + 2);
                    } else if len_before > 1 {
                        // Subsequent collision: send only the new file
                        let _ = hash_task_tx.send(file_path);
                        hash_bar.set_length(hash_bar.length().unwrap_or(0) + 1);
                    }
                }
            }
            Err(_) => {
                // Scanner thread done
                break;
            }
        }
    }

    scan_spinner.finish_and_clear();

    // Wait for scanner to finish and handle any errors
    let _ = scanner_handle.join();

    // Drop the hash_task_tx so workers know when to stop
    drop(hash_task_tx);

    // Wait for all workers to finish
    for handle in worker_handles {
        let _ = handle.join();
    }

    // Drop result_tx so we can collect results
    drop(result_tx);

    // Collect all hashing results
    let mut results: HashMap<Hash, Vec<PathBuf>> = HashMap::new();
    while let Ok((hash, path)) = result_rx.recv() {
        results.entry(hash).or_default().push(path);
    }

    hash_bar.finish_and_clear();

    // Set refcount for collisions
    for (hash, paths) in &results {
        if paths.len() > 1 {
            state.set_cas_refcount(hash, paths.len() as u64)?;
        }
    }

    Ok(results)
}

fn dedupe_groups(
    groups: &HashMap<Hash, Vec<PathBuf>>,
    state: &state::State,
    paranoid: bool,
    dry_run: bool,
    allow_unsafe_hardlinks: bool,
) -> Result<()> {
    let mut reflink_warning_shown = false;
    let mut warn_reflink_unsupported = |name: &str| {
        if !reflink_warning_shown {
            println!("\n{}", "━".repeat(80).yellow());
            println!("{} Filesystem Does Not Support Copy-on-Write Reflinks", "[WARNING]".bold().yellow());
            println!("{}", "━".repeat(80).yellow());
            println!("\nYour filesystem does not support CoW (Copy-on-Write) reflinks.");
            println!("Reflinks allow files to share disk space while remaining independent copies.");
            println!("When you modify a reflinked file, only the changed portions use new disk space.\n");
            println!("{}", "Key differences:".bold());
            println!("  • Reflinks: Different inodes, individual metadata, copy-on-write protection");
            println!("  • Hard links: Shared inode, shared metadata, direct data sharing\n");
            println!("{}", "Implications:".bold());
            println!("  • With hard links, modifying any file affects all linked copies and the vault master");
            println!("  • All hard-linked files share the same timestamps and permissions");
            println!("  • Hard links save disk space but require careful file handling\n");
            println!("{}", "Your options:".bold());
            println!("  1. {} - Files will be skipped (safe default)", "Do nothing".green());
            println!("  2. {} - Enables deduplication with shared metadata", "Add --allow-unsafe-hardlinks".yellow());
            println!("  3. {} - Btrfs, XFS (Linux), APFS (macOS), ReFS (Windows)\n", "Switch to a reflink-capable filesystem".cyan());
            println!("{}", "━".repeat(80).yellow());
            println!();
            reflink_warning_shown = true;
        }
        println!("{} {}", "[SKIPPED]".bold().red(), name);
    };
    
    for (hash, paths) in groups {
        if paths.len() < 2 {
            continue;
        }
        let master = &paths[0];
        
        // Handle master file: either move to vault or calculate theoretical path
        let vault_path = if dry_run {
            let theoretical_path = vault::shard_path(hash)?;
            let name = display_name(master);
            println!(
                "{} Would move master: {} -> {}",
                "[DRY RUN]".yellow().dimmed(),
                name,
                theoretical_path.display()
            );
            theoretical_path
        } else {
            vault::ensure_in_vault(hash, master)?
        };
        
        let mut master_verified = false;
        if paranoid && !dry_run && master.exists() {
            match dedupe::compare_files(&vault_path, master) {
                Ok(true) => master_verified = true,
                Ok(false) => {
                    eprintln!(
                        "HASH COLLISION OR BIT ROT DETECTED: {}",
                        master.display()
                    );
                    continue;
                }
                Err(err) => {
                    eprintln!("VERIFY FAILED (skipping): {}: {err}", master.display());
                    continue;
                }
            }
        }
        
        if paranoid && dry_run {
            println!(
                "{} Skipping paranoid verification (master not in vault)",
                "[DRY RUN]".yellow().dimmed()
            );
        }
        
        // Handle master file replacement (or dry-run simulation)
        if !dry_run {
            match dedupe::replace_with_link(&vault_path, master, allow_unsafe_hardlinks) {
                Ok(Some(link_type)) => {
                    if link_type == dedupe::LinkType::HardLink {
                        let inode = std::fs::metadata(master)?.ino();
                        state.mark_inode_vaulted(inode)?;
                    }
                    if !is_temp_file(master) {
                        let name = display_name(master);
                        match link_type {
                            dedupe::LinkType::Reflink => {
                                if paranoid && master_verified {
                                    println!(
                                        "{} {} {}",
                                        "[REFLINK ]".bold().green(),
                                        "[VERIFIED]".bold().blue(),
                                        name
                                    );
                                } else {
                                    println!("{} {}", "[REFLINK ]".bold().green(), name);
                                }
                            }
                            dedupe::LinkType::HardLink => {
                                if paranoid && master_verified {
                                    println!(
                                        "{} {} {}",
                                        "[HARDLINK]".bold().yellow(),
                                        "[VERIFIED]".bold().blue(),
                                        name
                                    );
                                } else {
                                    println!("{} {}", "[HARDLINK]".bold().yellow(), name);
                                }
                            }
                        }
                    }
                }
                Ok(None) => {}
                Err(e) => {
                    if e.to_string().contains("reflink not supported") {
                        if let Err(restore_err) = std::fs::rename(&vault_path, master) {
                            let copy_result = std::fs::copy(&vault_path, master)
                                .and_then(|_| std::fs::remove_file(&vault_path));
                            if let Err(copy_err) = copy_result {
                                eprintln!(
                                    "[ERROR] Failed to restore master from vault. File remains at {}. Rename error: {restore_err}. Copy/remove error: {copy_err}",
                                    vault_path.display()
                                );
                            }
                        }

                        let name = display_name(master);
                        warn_reflink_unsupported(&name);
                        continue;
                    } else {
                        return Err(e);
                    }
                }
            }
        } else {
            // Dry-run: simulate linking
            let name = display_name(master);
            println!(
                "{} Would dedupe: {} -> {} (reflink/hardlink)",
                "[DRY RUN]".yellow().dimmed(),
                name,
                vault_path.display()
            );
        }

        // Handle duplicates
        for path in paths.iter().skip(1) {
            let mut verified = false;
            if paranoid && !dry_run {
                match dedupe::compare_files(&vault_path, path) {
                    Ok(true) => verified = true,
                    Ok(false) => {
                        eprintln!(
                            "HASH COLLISION OR BIT ROT DETECTED: {}",
                            path.display()
                        );
                        continue;
                    }
                    Err(err) => {
                        eprintln!("VERIFY FAILED (skipping): {}: {err}", path.display());
                        continue;
                    }
                }
            }
            
            if !dry_run {
                match dedupe::replace_with_link(&vault_path, path, allow_unsafe_hardlinks) {
                    Ok(Some(link_type)) => {
                        if link_type == dedupe::LinkType::HardLink {
                            let inode = std::fs::metadata(path)?.ino();
                            state.mark_inode_vaulted(inode)?;
                        }
                        if !is_temp_file(path) {
                            let name = display_name(path);
                            match link_type {
                                dedupe::LinkType::Reflink => {
                                    if paranoid && verified {
                                        println!(
                                            "{} {} {}",
                                            "[REFLINK ]".bold().green(),
                                            "[VERIFIED]".bold().blue(),
                                            name
                                        );
                                    } else {
                                        println!("{} {}", "[REFLINK ]".bold().green(), name);
                                    }
                                }
                                dedupe::LinkType::HardLink => {
                                    if paranoid && verified {
                                        println!(
                                            "{} {} {}",
                                            "[HARDLINK]".bold().yellow(),
                                            "[VERIFIED]".bold().blue(),
                                            name
                                        );
                                    } else {
                                        println!("{} {}", "[HARDLINK]".bold().yellow(), name);
                                    }
                                }
                            }
                        }
                    }
                    Ok(None) => {}
                    Err(e) => {
                        if e.to_string().contains("reflink not supported") {
                            let name = display_name(path);
                            warn_reflink_unsupported(&name);
                            continue;
                        } else {
                            return Err(e);
                        }
                    }
                }
            } else {
                // Dry-run: simulate linking
                let name = display_name(path);
                println!(
                    "{} Would dedupe: {} -> {} (reflink/hardlink)",
                    "[DRY RUN]".yellow().dimmed(),
                    name,
                    vault_path.display()
                );
            }
        }
        
        // Handle database state updates (or dry-run simulation)
        if !dry_run {
            state.set_cas_refcount(hash, paths.len() as u64)?;
        } else {
            let hex = crate::types::hash_to_hex(hash);
            println!(
                "{} Would update DB state for hash {}",
                "[DRY RUN]".yellow().dimmed(),
                hex
            );
        }
    }
    Ok(())
}

fn display_name(path: &Path) -> String {
    path.file_name()
        .and_then(|name| name.to_str())
        .map(|name| name.to_string())
        .unwrap_or_else(|| path.display().to_string())
}

fn is_temp_file(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .map(|name| name.ends_with(".imprint_tmp"))
        .unwrap_or(false)
}

fn file_modified(path: &Path) -> Result<u64> {
    let metadata = std::fs::metadata(path).with_context(|| "read metadata")?;
    let modified = metadata.modified().with_context(|| "read modified time")?;
    let duration = modified
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    Ok(duration.as_secs())
}

fn progress(label: &str, total: u64) -> ProgressBar {
    let bar = ProgressBar::new(total);
    bar.set_style(
        ProgressStyle::with_template("{msg} [{bar:40.cyan/blue}] {pos}/{len}")
            .unwrap()
            .progress_chars("##-"),
    );
    bar.set_message(label.to_string());
    bar
}

fn print_summary(mode: &str, groups: &HashMap<Hash, Vec<PathBuf>>) {
    let duplicates = groups.values().filter(|g| g.len() > 1).count();
    println!("{mode} complete. duplicate groups: {duplicates}");
}

fn restore_pipeline(path: &Path, state: &state::State, dry_run: bool) -> Result<()> {
    let multi = MultiProgress::new();
    let restore_spinner = multi.add(ProgressBar::new_spinner());
    restore_spinner.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner} {msg}")
            .unwrap(),
    );
    restore_spinner.set_message("Scanning for deduplicated files to restore...");

    let mut restored_count = 0;
    let mut bytes_restored = 0;

    for entry in jwalk::WalkDir::new(path).into_iter() {
        let entry = match entry {
            Ok(entry) => entry,
            Err(_) => continue,
        };
        if !entry.file_type().is_file() {
            continue;
        }
        let file_path = entry.path();
        if is_temp_file(&file_path) {
            continue;
        }

        let metadata = match std::fs::metadata(&file_path) {
            Ok(m) => m,
            Err(_) => continue,
        };

        let inode = metadata.ino();
        let size = metadata.len();
        
        let mut needs_restore = false;
        let mut target_hash: Option<Hash> = None;

        // Condition 1: Hardlink detected in DB
        if state.is_inode_vaulted(inode).unwrap_or(false) {
            needs_restore = true;
            if let Ok(Some(file_meta)) = state.get_file_metadata(&file_path) {
                target_hash = Some(file_meta.hash);
            }
        } 
        // Condition 2: Reflink/File indexed in DB and matches vault
        else if let Ok(Some(file_meta)) = state.get_file_metadata(&file_path) {
            if let Ok(vault_path) = vault::shard_path(&file_meta.hash) {
                if vault_path.exists() {
                    needs_restore = true;
                    target_hash = Some(file_meta.hash);
                }
            }
        }

        if needs_restore {
            let name = display_name(&file_path);
            restore_spinner.set_message(format!("Restoring {name}..."));
            
            if dry_run {
                println!("{} Would restore: {}", "[DRY RUN]".yellow().dimmed(), name);
                if let Some(hash) = target_hash {
                    println!("{}   -> Would decrement refcount for {}", "[DRY RUN]".yellow().dimmed(), crate::types::hash_to_hex(&hash));
                }
                restored_count += 1;
                bytes_restored += size;
            } else {
                if dedupe::restore_file(&file_path).is_ok() {
                    println!("{} {}", "[RESTORED]".bold().cyan(), name);
                    
                    // DB Cleanup
                    let _ = state.unmark_inode_vaulted(inode);
                    let _ = state.remove_file_from_index(&file_path);
                    
                    // Refcount and Vault Cleanup
                    if let Some(hash) = target_hash {
                        if let Ok(mut current_refcount) = state.get_cas_refcount(&hash) {
                            if current_refcount > 0 {
                                current_refcount -= 1;
                                if current_refcount == 0 {
                                    let _ = vault::remove_from_vault(&hash);
                                    let _ = state.remove_cas_refcount(&hash);
                                    println!("{}    -> Vault copy pruned (refcount 0)", "[GC]".bold().magenta());
                                } else {
                                    let _ = state.set_cas_refcount(&hash, current_refcount);
                                }
                            }
                        }
                    }

                    restored_count += 1;
                    bytes_restored += size;
                } else {
                    eprintln!("{} Failed to restore {name}", "[ERROR]".bold().red());
                }
            }
        }
    }

    restore_spinner.finish_and_clear();
    println!(
        "Restore complete. Files restored: {} ({:.2} MB)", 
        restored_count, 
        bytes_restored as f64 / 1_048_576.0
    );
    Ok(())
}
