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
#[command(author, version, about = "Imprint - speed-first deduplication engine")]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Scan { path: PathBuf },
    Dedupe {
        path: PathBuf,
        #[arg(
            long,
            help = "Perform byte-for-byte verification before linking to guarantee 100% collision safety."
        )]
        paranoid: bool,
        #[arg(
            long,
            short = 'n',
            help = "Simulate operations without modifying the filesystem or database."
        )]
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
        Commands::Dedupe { path, paranoid, dry_run } => {
            let groups = scan_pipeline(&path, &state)?;
            dedupe_groups(&groups, &state, paranoid, dry_run)?;
            print_summary("dedupe", &groups);
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
) -> Result<()> {
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
            if let Some(link_type) = dedupe::replace_with_link(&vault_path, master)? {
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
                if let Some(link_type) = dedupe::replace_with_link(&vault_path, path)? {
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
