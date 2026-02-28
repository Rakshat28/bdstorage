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

use crate::state::DbOp;
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
    Scan {
        path: PathBuf,
    },
    Dedupe {
        path: PathBuf,
        #[arg(long)]
        paranoid: bool,
        #[arg(long, short = 'n')]
        dry_run: bool,
        #[arg(long, action = clap::ArgAction::SetTrue, default_value_t = false)]
        allow_unsafe_hardlinks: bool,
    },
    Restore {
        path: PathBuf,
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

    match args.command {
        Commands::Scan { path } => {
            let state = state::State::open_default()?;
            let groups = scan_pipeline(&path, &state)?;
            print_summary("scan", &groups);
        }
        Commands::Dedupe {
            path,
            paranoid,
            dry_run,
            allow_unsafe_hardlinks,
        } => {
            let state = if dry_run {
                state::State::open_readonly_if_exists()?
            } else {
                state::State::open_default()?
            };
            let groups = scan_pipeline(&path, &state)?;
            dedupe_groups(&groups, &state, paranoid, dry_run, allow_unsafe_hardlinks)?;
            print_summary("dedupe", &groups);
        }
        Commands::Restore { path, dry_run } => {
            let state = if dry_run {
                state::State::open_readonly_if_exists()?
            } else {
                state::State::open_default()?
            };
            restore_pipeline(&path, &state, dry_run)?;
        }
    }

    Ok(())
}

fn scan_pipeline(path: &Path, state: &state::State) -> Result<HashMap<Hash, Vec<PathBuf>>> {
    let multi = MultiProgress::new();
    let scan_spinner = multi.add(ProgressBar::new_spinner());
    scan_spinner.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner} {msg}")
            .unwrap(),
    );
    scan_spinner.set_message("Scanning...");

    let hash_bar = multi.add(progress("Indexing/Hashing", 0));

    let (scan_tx, scan_rx) = channel::unbounded();
    let path_clone = path.to_path_buf();
    let scanner_handle =
        std::thread::spawn(move || -> Result<()> { scanner::stream_scan(&path_clone, scan_tx) });

    let (hash_task_tx, hash_task_rx) = channel::unbounded::<PathBuf>();

    let (result_tx, result_rx) = channel::unbounded::<(Hash, PathBuf)>();

    let (db_tx, db_rx) = channel::unbounded::<DbOp>();

    let state_clone = state.clone();
    let num_workers = std::cmp::min(rayon::current_num_threads(), 8);
    let mut worker_handles = vec![];

    for _ in 0..num_workers {
        let rx = hash_task_rx.clone();
        let tx = result_tx.clone();
        let db_ops_tx = db_tx.clone();
        let state_ref = state_clone.clone();
        let hash_bar_clone = hash_bar.clone();

        let handle = std::thread::spawn(move || {
            while let Ok(file_path) = rx.recv() {
                if let Ok(metadata) = std::fs::metadata(&file_path) {
                    let inode = metadata.ino();
                    if let Ok(is_vaulted) = state_ref.is_inode_vaulted(inode)
                        && is_vaulted
                    {
                        continue;
                    }

                    let size = metadata.len();
                    if let Ok(_) = hasher::sparse_hash(&file_path, size)
                        && let Ok(full_hash) = hasher::full_hash(&file_path)
                    {
                        let modified = file_modified(&file_path).unwrap_or(0);
                        let file_metadata = FileMetadata {
                            size,
                            modified,
                            hash: full_hash,
                        };
                        let _ = db_ops_tx.send(DbOp::UpsertFile(file_path.clone(), file_metadata));
                        let _ = tx.send((full_hash, file_path));
                        hash_bar_clone.inc(1);
                    }
                }
            }
        });

        worker_handles.push(handle);
    }

    let state_db_writer = state_clone.clone();
    let db_writer_handle = std::thread::spawn(move || {
        state_db_writer.batch_write_from_channel(db_rx);
    });

    let mut size_map: HashMap<u64, Vec<PathBuf>> = HashMap::new();

    while let Ok(file_path) = scan_rx.recv() {
        scan_spinner.tick();

        if let Ok(metadata) = std::fs::metadata(&file_path) {
            let size = metadata.len();
            let entry = size_map.entry(size).or_default();
            let len_before = entry.len();
            entry.push(file_path.clone());

            if len_before == 1 {
                if let Some(first_file) = entry.first().cloned() {
                    let _ = hash_task_tx.send(first_file);
                }
                let _ = hash_task_tx.send(file_path);
                hash_bar.set_length(hash_bar.length().unwrap_or(0) + 2);
            } else if len_before > 1 {
                let _ = hash_task_tx.send(file_path);
                hash_bar.set_length(hash_bar.length().unwrap_or(0) + 1);
            }
        }
    }

    scan_spinner.finish_and_clear();

    let _ = scanner_handle.join();

    drop(hash_task_tx);

    for handle in worker_handles {
        let _ = handle.join();
    }

    drop(result_tx);
    drop(db_tx);

    let mut results: HashMap<Hash, Vec<PathBuf>> = HashMap::new();
    while let Ok((hash, path)) = result_rx.recv() {
        results.entry(hash).or_default().push(path);
    }

    hash_bar.finish_and_clear();

    let _ = db_writer_handle.join();

    let mut refcount_ops = Vec::new();
    for (hash, paths) in &results {
        if paths.len() > 1 {
            refcount_ops.push(DbOp::SetCasRefcount(*hash, paths.len() as u64));
        }
    }
    if !refcount_ops.is_empty() {
        state.batch_write(refcount_ops)?;
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
    let mut global_db_ops = Vec::new();
    let mut reflink_warning_shown = false;
    let mut warn_reflink_unsupported = |name: &str| {
        if !reflink_warning_shown {
            println!("\n{}", "━".repeat(80).yellow());
            println!(
                "{} Filesystem Does Not Support Copy-on-Write Reflinks",
                "[WARNING]".bold().yellow()
            );
            println!("{}", "━".repeat(80).yellow());
            println!("\nYour filesystem does not support CoW (Copy-on-Write) reflinks.");
            println!(
                "Reflinks allow files to share disk space while remaining independent copies."
            );
            println!(
                "When you modify a reflinked file, only the changed portions use new disk space.\n"
            );
            println!("{}", "Key differences:".bold());
            println!(
                "  • Reflinks: Different inodes, individual metadata, copy-on-write protection"
            );
            println!("  • Hard links: Shared inode, shared metadata, direct data sharing\n");
            println!("{}", "Implications:".bold());
            println!(
                "  • With hard links, modifying any file affects all linked copies and the vault master"
            );
            println!("  • All hard-linked files share the same timestamps and permissions");
            println!("  • Hard links save disk space but require careful file handling\n");
            println!("{}", "Your options:".bold());
            println!(
                "  1. {} - Files will be skipped (safe default)",
                "Do nothing".green()
            );
            println!(
                "  2. {} - Enables deduplication with shared metadata",
                "Add --allow-unsafe-hardlinks".yellow()
            );
            println!(
                "  3. {} - Btrfs, XFS (Linux), APFS (macOS), ReFS (Windows)\n",
                "Switch to a reflink-capable filesystem".cyan()
            );
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
                    eprintln!("HASH COLLISION OR BIT ROT DETECTED: {}", master.display());
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

        let mut db_ops = Vec::new();

        if !dry_run {
            match dedupe::replace_with_link(&vault_path, master, allow_unsafe_hardlinks) {
                Ok(Some(link_type)) => {
                    if link_type == dedupe::LinkType::HardLink {
                        let inode = std::fs::metadata(master)?.ino();
                        db_ops.push(DbOp::MarkInodeVaulted(inode));
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
            let name = display_name(master);
            println!(
                "{} Would dedupe: {} -> {} (reflink/hardlink)",
                "[DRY RUN]".yellow().dimmed(),
                name,
                vault_path.display()
            );
        }

        for path in paths.iter().skip(1) {
            let mut verified = false;
            if paranoid && !dry_run {
                match dedupe::compare_files(&vault_path, path) {
                    Ok(true) => verified = true,
                    Ok(false) => {
                        eprintln!("HASH COLLISION OR BIT ROT DETECTED: {}", path.display());
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
                            db_ops.push(DbOp::MarkInodeVaulted(inode));
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
                let name = display_name(path);
                println!(
                    "{} Would dedupe: {} -> {} (reflink/hardlink)",
                    "[DRY RUN]".yellow().dimmed(),
                    name,
                    vault_path.display()
                );
            }
        }

        if !dry_run {
            db_ops.push(DbOp::SetCasRefcount(*hash, paths.len() as u64));
            global_db_ops.extend(db_ops);
            if global_db_ops.len() >= 1000 {
                state.batch_write(std::mem::take(&mut global_db_ops))?;
            }
        } else {
            let hex = crate::types::hash_to_hex(hash);
            println!(
                "{} Would update DB state for hash {}",
                "[DRY RUN]".yellow().dimmed(),
                hex
            );
        }
    }

    if !dry_run && !global_db_ops.is_empty() {
        state.batch_write(global_db_ops)?;
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
    let mut global_restore_ops = Vec::new();

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

        if state.is_inode_vaulted(inode).unwrap_or(false) {
            needs_restore = true;
            if let Ok(Some(file_meta)) = state.get_file_metadata(&file_path) {
                target_hash = Some(file_meta.hash);
            }
        } else if let Ok(Some(file_meta)) = state.get_file_metadata(&file_path)
            && let Ok(vault_path) = vault::shard_path(&file_meta.hash)
            && vault_path.exists()
        {
            needs_restore = true;
            target_hash = Some(file_meta.hash);
        }

        if needs_restore {
            let name = display_name(&file_path);
            restore_spinner.set_message(format!("Restoring {name}..."));

            if dry_run {
                println!("{} Would restore: {}", "[DRY RUN]".yellow().dimmed(), name);
                if let Some(hash) = target_hash {
                    println!(
                        "{}   -> Would decrement refcount for {}",
                        "[DRY RUN]".yellow().dimmed(),
                        crate::types::hash_to_hex(&hash)
                    );
                }
                restored_count += 1;
                bytes_restored += size;
            } else if dedupe::restore_file(&file_path).is_ok() {
                println!("{} {}", "[RESTORED]".bold().cyan(), name);

                let mut restore_ops = vec![
                    DbOp::UnmarkInodeVaulted(inode),
                    DbOp::RemoveFileFromIndex(file_path.clone()),
                ];

                if let Some(hash) = target_hash
                    && let Ok(mut current_refcount) = state.get_cas_refcount(&hash)
                    && current_refcount > 0
                {
                    current_refcount -= 1;
                    if current_refcount == 0 {
                        let _ = vault::remove_from_vault(&hash);
                        restore_ops.push(DbOp::RemoveCasRefcount(hash));
                        println!(
                            "{}    -> Vault copy pruned (refcount 0)",
                            "[GC]".bold().magenta()
                        );
                    } else {
                        restore_ops.push(DbOp::SetCasRefcount(hash, current_refcount));
                    }
                }
                global_restore_ops.extend(restore_ops);
                if global_restore_ops.len() >= 1000 {
                    let _ = state.batch_write(std::mem::take(&mut global_restore_ops));
                }

                restored_count += 1;
                bytes_restored += size;
            } else {
                eprintln!("{} Failed to restore {name}", "[ERROR]".bold().red());
            }
        }
    }

    if !global_restore_ops.is_empty() {
        let _ = state.batch_write(global_restore_ops);
    }

    restore_spinner.finish_and_clear();
    println!(
        "Restore complete. Files restored: {} ({:.2} MB)",
        restored_count,
        bytes_restored as f64 / 1_048_576.0
    );
    Ok(())
}
