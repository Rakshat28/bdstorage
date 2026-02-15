mod dedupe;
mod hasher;
mod scanner;
mod state;
mod types;
mod vault;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use colored::*;
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
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
    Dedupe { path: PathBuf },
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
        Commands::Dedupe { path } => {
            let groups = scan_pipeline(&path, &state)?;
            dedupe_groups(&groups, &state)?;
            print_summary("dedupe", &groups);
        }
    }

    Ok(())
}

fn scan_pipeline(path: &Path, state: &state::State) -> Result<HashMap<Hash, Vec<PathBuf>>> {
    let size_groups = scanner::group_by_size(path)?;
    let size_groups: Vec<Vec<PathBuf>> = size_groups
        .into_values()
        .filter(|paths| paths.len() > 1)
        .collect();

    let sparse_bar = progress("sparse hashing", size_groups.len() as u64);
    let mut sparse_groups: Vec<Vec<PathBuf>> = Vec::new();

    for group in size_groups {
        sparse_bar.inc(1);
        let sparse_hashes: Vec<(Hash, PathBuf)> = group
            .par_iter()
            .map(|path| -> Result<Option<(Hash, PathBuf)>> {
                let meta = std::fs::metadata(path)?;
                let inode = meta.ino();
                if state.is_inode_vaulted(inode)? {
                    return Ok(None);
                }
                let hash = hasher::sparse_hash(path, meta.len())?;
                Ok(Some((hash, path.clone())))
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect();

        let mut buckets: HashMap<Hash, Vec<PathBuf>> = HashMap::new();
        for (hash, path) in sparse_hashes {
            buckets.entry(hash).or_default().push(path);
        }
        for (_, paths) in buckets {
            if paths.len() > 1 {
                sparse_groups.push(paths);
            }
        }
    }
    sparse_bar.finish_and_clear();

    let total_full: usize = sparse_groups.iter().map(|g| g.len()).sum();
    let full_bar = progress("full hashing", total_full as u64);
    let mut full_groups: HashMap<Hash, Vec<PathBuf>> = HashMap::new();

    for group in sparse_groups {
        let full_hashes: Vec<(Hash, PathBuf, u64)> = group
            .par_iter()
            .map(|path| -> Result<(Hash, PathBuf, u64)> {
                let meta = std::fs::metadata(path)?;
                let hash = hasher::full_hash(path)?;
                Ok((hash, path.clone(), meta.len()))
            })
            .collect::<Result<Vec<_>>>()?;

        for (hash, path, size) in full_hashes {
            full_bar.inc(1);
            let modified = file_modified(path.as_path())?;
            let metadata = FileMetadata {
                size,
                modified,
                hash,
            };
            state.upsert_file(&path, &metadata)?;
            full_groups.entry(hash).or_default().push(path);
        }
    }
    full_bar.finish_and_clear();

    for (hash, paths) in &full_groups {
        if paths.len() > 1 {
            state.set_cas_refcount(hash, paths.len() as u64)?;
        }
    }

    Ok(full_groups)
}

fn dedupe_groups(groups: &HashMap<Hash, Vec<PathBuf>>, state: &state::State) -> Result<()> {
    for (hash, paths) in groups {
        if paths.len() < 2 {
            continue;
        }
        let master = &paths[0];
        let vault_path = vault::ensure_in_vault(hash, master)?;
        if let Some(link_type) = dedupe::replace_with_link(&vault_path, master)? {
            if link_type == dedupe::LinkType::HardLink {
                let inode = std::fs::metadata(master)?.ino();
                state.mark_inode_vaulted(inode)?;
            }
            if !is_temp_file(master) {
                let name = display_name(master);
                match link_type {
                    dedupe::LinkType::Reflink => {
                        println!("{} {}", "[REFLINK ]".bold().green(), name);
                    }
                    dedupe::LinkType::HardLink => {
                        println!("{} {}", "[HARDLINK]".bold().yellow(), name);
                    }
                }
            }
        }

        for path in paths.iter().skip(1) {
            if let Some(link_type) = dedupe::replace_with_link(&vault_path, path)? {
                if link_type == dedupe::LinkType::HardLink {
                    let inode = std::fs::metadata(path)?.ino();
                    state.mark_inode_vaulted(inode)?;
                }
                if !is_temp_file(path) {
                    let name = display_name(path);
                    match link_type {
                        dedupe::LinkType::Reflink => {
                            println!("{} {}", "[REFLINK ]".bold().green(), name);
                        }
                        dedupe::LinkType::HardLink => {
                            println!("{} {}", "[HARDLINK]".bold().yellow(), name);
                        }
                    }
                }
            }
        }
        state.set_cas_refcount(hash, paths.len() as u64)?;
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
