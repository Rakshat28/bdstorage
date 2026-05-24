mod dedupe;
mod hasher;
mod scanner;
mod state;
#[cfg(not(windows))]
mod systemd;
mod types;
mod vault;

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand, ValueEnum};
use colored::*;
use crossbeam::channel;
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use std::collections::HashMap;
use std::io::IsTerminal;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::state::DbOp;
use crate::types::{FileMetadata, Hash, JsonReport, ProcessingError};

#[derive(Parser, Debug)]
#[command(
    name = "bdstorage",
    author,
    version,
    about = "bdstorage: A speed-first, local file deduplication engine.",
    long_about = "bdstorage uses a Tiered Hashing philosophy to minimize I/O overhead:\n\nSize Grouping: Eliminates unique file sizes immediately.\n\nSparse Hashing: Samples 12KB (start/middle/end) to identify candidates.\n\nFull BLAKE3 Hashing: Verifies matches with high-performance 128KB buffering.",
    help_template = "{before-help}{name} {version}\n{author-with-newline}{about-section}\n\nSTORAGE PATHS (override: --vault-dir or $BDSTORAGE_VAULT):\n  State DB: <vault-dir>/state.redb  (default: ~/.imprint/state.redb)\n  CAS Vault: <vault-dir>/store/     (default: ~/.imprint/store/)\n\n{usage-heading} {usage}\n\nGLOBAL FLAGS:\n  --vault-dir <PATH>           Override vault/state directory (env: BDSTORAGE_VAULT)\n  --output-format <text|json>  Set output format (default: text)\n  -q, --quiet                  Suppress all progress output\n  -h, --help                   Print help\n  -V, --version                Print version\n\nSUBCOMMAND FLAGS:\n  --paranoid                 Available on the dedupe subcommand. Forces a byte-for-byte\n                             verification before linking to guarantee 100% collision safety.\n\n  --allow-unsafe-hardlinks   Available on the dedupe subcommand. Allows hard link fallback\n                             when CoW reflinks are not supported. Hard links share the same\n                             inode, so all linked files will have identical metadata.\n\n  -n, --dry-run              Available on dedupe and restore subcommands. Simulates operations\n                             without modifying the filesystem or the database.\n\n{all-args}{after-help}"
)]
struct Cli {
    /// Override the vault and state directory.
    /// Falls back to $BDSTORAGE_VAULT env var, then $HOME/.imprint/.
    #[arg(long, global = true, env = "BDSTORAGE_VAULT")]
    vault_dir: Option<PathBuf>,

    #[arg(long, global = true, value_enum, default_value_t = OutputFormat::Text)]
    output_format: OutputFormat,

    /// Suppress all progress output.
    #[arg(long, short = 'q', global = true)]
    quiet: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Default)]
enum OutputFormat {
    #[default]
    Text,
    Json,
}

#[derive(Args, Debug, Clone)]
struct DedupeOptions {
    path: PathBuf,
    #[arg(long)]
    paranoid: bool,
    #[arg(long, short = 'n')]
    dry_run: bool,
    #[arg(long, action = clap::ArgAction::SetTrue, default_value_t = false)]
    allow_unsafe_hardlinks: bool,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Scan {
        path: PathBuf,
    },
    Dedupe(DedupeOptions),
    #[command(about = "Daemon utilities: run periodic dedupe or install a systemd service unit.")]
    Daemon {
        #[command(subcommand)]
        command: DaemonCommand,
    },
    Restore {
        path: PathBuf,
        #[arg(long, short = 'n')]
        dry_run: bool,
    },
    #[command(
        about = "Print a summary of the current vault state and space savings (no scan required)."
    )]
    Status {
        #[arg(long, action = clap::ArgAction::SetTrue)]
        json: bool,
    },
}

#[derive(Subcommand, Debug)]
enum DaemonCommand {
    #[command(about = "Periodically run dedupe until SIGINT or SIGTERM.")]
    Run(DaemonRunOptions),
    #[command(about = "Generate/install a systemd service for daemon mode.")]
    Install(DaemonInstallOptions),
}

#[derive(Args, Debug, Clone)]
struct DaemonRunOptions {
    #[command(flatten)]
    dedupe: DedupeOptions,
    #[arg(long, default_value_t = 3600)]
    interval_secs: u64,
}

#[derive(Args, Debug, Clone)]
struct DaemonInstallOptions {
    #[arg(long)]
    target: PathBuf,
    #[arg(long, default_value_t = 3600)]
    interval_secs: u64,
    #[arg(long, action = clap::ArgAction::SetTrue, default_value_t = false)]
    allow_unsafe_hardlinks: bool,
}

#[derive(Debug, Clone, Copy, Default)]
struct DedupeRunSummary {
    duplicate_groups: usize,
    files_linked: usize,
}

fn resolve_vault_dir(cli_override: Option<&Path>) -> Result<PathBuf> {
    match cli_override {
        Some(p) => Ok(p.to_path_buf()),
        None => {
            let home = std::env::var("HOME")
                .or_else(|_| std::env::var("USERPROFILE"))
                .with_context(|| "Neither HOME nor USERPROFILE is set")?;
            Ok(PathBuf::from(home).join(".imprint"))
        }
    }
}

fn main() {
    if let Err(err) = run() {
        eprintln!("{err:?}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let args = Cli::parse();
    let quiet = args.quiet || !std::io::stdout().is_terminal();
    hasher::set_quiet(quiet);

    let vault_dir = resolve_vault_dir(args.vault_dir.as_deref())?;
    let mut report = JsonReport::default();

    match args.command {
        Commands::Scan { path } => {
            let state = state::State::open_default(&vault_dir)?;
            let groups = scan_pipeline(&path, &state, args.output_format, &mut report)?;
            if args.output_format == OutputFormat::Text {
                print_summary("scan", &groups);
            }
        }
        Commands::Dedupe(opts) => {
            let summary = run_dedupe_once(&opts, &vault_dir, args.output_format, &mut report)?;
            if args.output_format == OutputFormat::Text {
                println!(
                    "dedupe complete. duplicate groups: {} files linked: {}",
                    summary.duplicate_groups, summary.files_linked
                );
            }
        }
        Commands::Daemon { command } => match command {
            DaemonCommand::Run(opts) => run_daemon(opts.dedupe, opts.interval_secs, &vault_dir)?,
            DaemonCommand::Install(opts) => {
                #[cfg(not(windows))]
                systemd::install_daemon_service(
                    &opts.target,
                    opts.interval_secs,
                    opts.allow_unsafe_hardlinks,
                )?;
                #[cfg(windows)]
                {
                    let _ = opts;
                    anyhow::bail!("systemd is not supported on Windows");
                }
            }
        },
        Commands::Restore { path, dry_run } => {
            let state = if dry_run {
                state::State::open_readonly_if_exists(&vault_dir)?
            } else {
                state::State::open_default(&vault_dir)?
            };
            restore_pipeline(&path, &state, dry_run, &vault_dir)?;
        }
        Commands::Status { json } => {
            let db_path = state::db_path(&vault_dir);
            if !db_path.exists() {
                anyhow::bail!(
                    "{} No vault found.\n{}",
                    "[ERROR]".bold().red(),
                    "Run 'bdstorage dedupe <path>' first to create the vault."
                );
            }
            let state = state::State::open_readonly_if_exists(&vault_dir)?;
            let vault_path = vault::vault_root(&vault_dir);
            let summary = state.compute_summary(&vault_path)?;

            if json || args.output_format == OutputFormat::Json {
                println!("{}", serde_json::to_string_pretty(&summary)?);
                return Ok(());
            }

            print_vault_status(&summary);
            return Ok(());
        }
    }

    if args.output_format == OutputFormat::Json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    }

    Ok(())
}

fn run_dedupe_once(
    opts: &DedupeOptions,
    vault_dir: &Path,
    format: OutputFormat,
    report: &mut JsonReport,
) -> Result<DedupeRunSummary> {
    let state = if opts.dry_run {
        state::State::open_readonly_if_exists(vault_dir)?
    } else {
        state::State::open_default(vault_dir)?
    };
    let groups = scan_pipeline(&opts.path, &state, format, report)?;
    dedupe_groups(
        &groups,
        &state,
        opts.paranoid,
        opts.dry_run,
        opts.allow_unsafe_hardlinks,
        vault_dir,
        format,
        report,
    )
}

fn run_daemon(opts: DedupeOptions, interval_secs: u64, vault_dir: &Path) -> Result<()> {
    let (shutdown_tx, shutdown_rx) = channel::bounded::<()>(1);
    ctrlc::set_handler(move || {
        let _ = shutdown_tx.try_send(());
    })
    .context("register signal handler")?;

    println!(
        "[daemon] started; target={} interval_secs={interval_secs}",
        opts.path.display()
    );

    let mut run_no: u64 = 1;
    loop {
        if shutdown_rx.try_recv().is_ok() {
            break;
        }

        println!("[daemon] run #{run_no} started");
        let started = std::time::Instant::now();
        let mut daemon_report = JsonReport::default();
        match run_dedupe_once(&opts, vault_dir, OutputFormat::Text, &mut daemon_report) {
            Ok(summary) => {
                println!(
                    "[daemon] run #{run_no} complete in {:.2}s; duplicate_groups={} files_linked={}",
                    started.elapsed().as_secs_f64(),
                    summary.duplicate_groups,
                    summary.files_linked,
                );
            }
            Err(err) => {
                eprintln!(
                    "[daemon] run #{run_no} failed in {:.2}s: {err:#}",
                    started.elapsed().as_secs_f64()
                );
            }
        }
        run_no += 1;

        match shutdown_rx.recv_timeout(Duration::from_secs(interval_secs)) {
            Ok(_) | Err(channel::RecvTimeoutError::Disconnected) => break,
            Err(channel::RecvTimeoutError::Timeout) => {}
        }
    }

    println!("[daemon] stopped");
    Ok(())
}

fn scan_pipeline(
    path: &Path,
    state: &state::State,
    format: OutputFormat,
    report: &mut JsonReport,
) -> Result<HashMap<Hash, Vec<PathBuf>>> {
    let is_json = format == OutputFormat::Json;
    let quiet = is_json || hasher::is_quiet();
    let multi = MultiProgress::with_draw_target(ProgressDrawTarget::stderr());
    let scan_spinner = if quiet {
        ProgressBar::hidden()
    } else {
        multi.add(ProgressBar::new_spinner())
    };
    scan_spinner.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner} {msg}")
            .unwrap(),
    );
    scan_spinner.set_message("Scanning...");

    let hash_bar = if quiet {
        ProgressBar::hidden()
    } else {
        multi.add(byte_progress("Indexing/Hashing", 0))
    };

    let (scan_tx, scan_rx) = channel::unbounded();
    let path_clone = path.to_path_buf();
    let scanner_handle =
        std::thread::spawn(move || -> Result<()> { scanner::stream_scan(&path_clone, scan_tx) });

    let (hash_task_tx, hash_task_rx) = channel::unbounded::<PathBuf>();

    let (result_tx, result_rx) = channel::unbounded::<Result<(Hash, PathBuf), (PathBuf, String)>>();

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
                    let inode = get_inode(&metadata);
                    if inode != 0 && state_ref.is_inode_vaulted(inode).unwrap_or(false) {
                        continue;
                    }

                    let size = metadata.len();
                    match hasher::sparse_hash(&file_path, size) {
                        Ok(_) => match hasher::full_hash(&file_path, Some(&hash_bar_clone)) {
                            Ok(full_hash) => {
                                let modified = file_modified(&file_path).unwrap_or(0);
                                let file_metadata = FileMetadata {
                                    size,
                                    modified,
                                    hash: full_hash,
                                };
                                let _ = db_ops_tx
                                    .send(DbOp::UpsertFile(file_path.clone(), file_metadata));
                                let _ = tx.send(Ok((full_hash, file_path)));
                            }
                            Err(e) => {
                                let _ = tx.send(Err((file_path, format!("Full hash failed: {e}"))));
                            }
                        },
                        Err(e) => {
                            let _ = tx.send(Err((file_path, format!("Sparse hash failed: {e}"))));
                        }
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

    while let Ok(res) = scan_rx.recv() {
        scan_spinner.tick();
        match res {
            Ok(file_path) => {
                if let Ok(metadata) = std::fs::metadata(&file_path) {
                    let size = metadata.len();
                    report.files_scanned += 1;

                    let entry = size_map.entry(size).or_default();
                    let len_before = entry.len();
                    entry.push(file_path.clone());

                    if len_before == 1 {
                        if let Some(first_file) = entry.first().cloned() {
                            let first_size =
                                std::fs::metadata(&first_file).map(|m| m.len()).unwrap_or(0);
                            hash_bar.set_length(hash_bar.length().unwrap_or(0) + first_size);
                            let _ = hash_task_tx.send(first_file);
                        }
                        hash_bar.set_length(hash_bar.length().unwrap_or(0) + size);
                        let _ = hash_task_tx.send(file_path);
                    } else if len_before > 1 {
                        hash_bar.set_length(hash_bar.length().unwrap_or(0) + size);
                        let _ = hash_task_tx.send(file_path);
                    }
                }
            }
            Err((path, reason)) => {
                report.errors.push(ProcessingError {
                    path: path.display().to_string(),
                    reason,
                });
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
    while let Ok(res) = result_rx.recv() {
        match res {
            Ok((hash, path)) => {
                results.entry(hash).or_default().push(path);
            }
            Err((path, reason)) => {
                report.errors.push(ProcessingError {
                    path: path.display().to_string(),
                    reason,
                });
            }
        }
    }

    report.duplicate_groups = results.values().filter(|g| g.len() > 1).count();

    hash_bar.finish_and_clear();

    let _ = db_writer_handle.join();

    Ok(results)
}

#[allow(clippy::too_many_arguments)]
fn dedupe_groups(
    groups: &HashMap<Hash, Vec<PathBuf>>,
    state: &state::State,
    paranoid: bool,
    dry_run: bool,
    allow_unsafe_hardlinks: bool,
    vault_dir: &Path,
    format: OutputFormat,
    report: &mut JsonReport,
) -> Result<DedupeRunSummary> {
    let is_json = format == OutputFormat::Json;
    let mut global_db_ops = Vec::new();
    let mut files_linked = 0usize;
    let duplicate_groups = groups.values().filter(|g| g.len() > 1).count();
    report.duplicate_groups = duplicate_groups;

    let mut reflink_warning_shown = false;
    let mut warn_reflink_unsupported = |name: &str| {
        if is_json {
            return;
        }
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
        if !is_json {
            println!("{} {}", "[SKIPPED]".bold().red(), name);
        }
    };

    for (hash, paths) in groups {
        if paths.len() < 2 {
            continue;
        }
        let master = &paths[0];

        let vault_path = if dry_run {
            let theoretical_path = vault::shard_path(vault_dir, hash);
            let name = display_name(master);
            if !is_json {
                println!(
                    "{} Would move master: {} -> {}",
                    "[DRY RUN]".yellow().dimmed(),
                    name,
                    theoretical_path.display()
                );
            }
            theoretical_path
        } else {
            let dest = vault::shard_path(vault_dir, hash);
            if !dest.exists() {
                report.vault_objects_added += 1;
            }
            vault::ensure_in_vault(vault_dir, hash, master)?
        };

        let mut master_verified = false;
        if paranoid && !dry_run && master.exists() {
            match dedupe::compare_files(&vault_path, master) {
                Ok(true) => master_verified = true,
                Ok(false) => {
                    if !is_json {
                        eprintln!("HASH COLLISION OR BIT ROT DETECTED: {}", master.display());
                    }
                    report.errors.push(ProcessingError {
                        path: master.display().to_string(),
                        reason: "Hash collision or bit rot detected".to_string(),
                    });
                    continue;
                }
                Err(err) => {
                    if !is_json {
                        eprintln!("VERIFY FAILED (skipping): {}: {err}", master.display());
                    }
                    report.errors.push(ProcessingError {
                        path: master.display().to_string(),
                        reason: format!("Verify failed: {err}"),
                    });
                    continue;
                }
            }
        }

        if paranoid && dry_run && !is_json {
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
                        let inode = get_inode(&std::fs::metadata(master)?);
                        db_ops.push(DbOp::MarkInodeVaulted(inode));
                    }
                    if !is_temp_file(master) {
                        let name = display_name(master);
                        if !is_json {
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
                        files_linked += 1;
                        report.links_created += 1;
                        // We DON'T add master size to bytes_saved to avoid overcounting.
                        // Saved space = (original files) - (vault copy).
                        // If we have 2 files, saved space = 1 file size.
                    }
                }
                Ok(None) => {}
                Err(e) => {
                    if e.to_string().contains("reflink not supported") {
                        if let Err(restore_err) = std::fs::rename(&vault_path, master) {
                            let copy_result = std::fs::copy(&vault_path, master)
                                .and_then(|_| std::fs::remove_file(&vault_path));
                            if let Err(copy_err) = copy_result
                                && !is_json
                            {
                                eprintln!(
                                    "[ERROR] Failed to restore master from vault. File remains at {}. Rename error: {restore_err}. Copy/remove error: {copy_err}",
                                    vault_path.display()
                                );
                            }
                        }

                        let name = display_name(master);
                        warn_reflink_unsupported(&name);
                        report.errors.push(ProcessingError {
                            path: master.display().to_string(),
                            reason: "Reflink not supported and hardlinks not allowed".to_string(),
                        });
                        continue;
                    } else {
                        report.errors.push(ProcessingError {
                            path: master.display().to_string(),
                            reason: format!("Dedupe failed: {e}"),
                        });
                        return Err(e);
                    }
                }
            }
        } else {
            if !is_json {
                let name = display_name(master);
                println!(
                    "{} Would dedupe: {} -> {} (reflink/hardlink)",
                    "[DRY RUN]".yellow().dimmed(),
                    name,
                    vault_path.display()
                );
            }
            files_linked += 1;
            report.links_created += 1;
        }

        for path in paths.iter().skip(1) {
            let mut verified = false;
            if paranoid && !dry_run {
                match dedupe::compare_files(&vault_path, path) {
                    Ok(true) => verified = true,
                    Ok(false) => {
                        if !is_json {
                            eprintln!("HASH COLLISION OR BIT ROT DETECTED: {}", path.display());
                        }
                        report.errors.push(ProcessingError {
                            path: path.display().to_string(),
                            reason: "Hash collision or bit rot detected".to_string(),
                        });
                        continue;
                    }
                    Err(err) => {
                        if !is_json {
                            eprintln!("VERIFY FAILED (skipping): {}: {err}", path.display());
                        }
                        report.errors.push(ProcessingError {
                            path: path.display().to_string(),
                            reason: format!("Verify failed: {err}"),
                        });
                        continue;
                    }
                }
            }

            if !dry_run {
                match dedupe::replace_with_link(&vault_path, path, allow_unsafe_hardlinks) {
                    Ok(Some(link_type)) => {
                        if link_type == dedupe::LinkType::HardLink {
                            let inode = get_inode(&std::fs::metadata(path)?);
                            db_ops.push(DbOp::MarkInodeVaulted(inode));
                        }
                        if !is_temp_file(path) {
                            let name = display_name(path);
                            if !is_json {
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
                            report.links_created += 1;
                            if let Ok(meta) = std::fs::metadata(path) {
                                report.bytes_saved += meta.len();
                            }
                        }
                        files_linked += 1;
                    }
                    Ok(None) => {}
                    Err(e) => {
                        if e.to_string().contains("reflink not supported") {
                            let name = display_name(path);
                            warn_reflink_unsupported(&name);
                            report.errors.push(ProcessingError {
                                path: path.display().to_string(),
                                reason: "Reflink not supported and hardlinks not allowed"
                                    .to_string(),
                            });
                            continue;
                        } else {
                            report.errors.push(ProcessingError {
                                path: path.display().to_string(),
                                reason: format!("Dedupe failed for duplicate: {e}"),
                            });
                            continue;
                        }
                    }
                }
            } else {
                let name = display_name(path);
                if !is_json {
                    println!(
                        "{} Would dedupe: {} -> {} (reflink/hardlink)",
                        "[DRY RUN]".yellow().dimmed(),
                        name,
                        vault_path.display()
                    );
                }
                files_linked += 1;
                report.links_created += 1;
                if let Ok(meta) = std::fs::metadata(path) {
                    report.bytes_saved += meta.len();
                }
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
            if !is_json {
                println!(
                    "{} Would update DB state for hash {}",
                    "[DRY RUN]".yellow().dimmed(),
                    hex
                );
            }
        }
    }

    if !dry_run && !global_db_ops.is_empty() {
        state.batch_write(global_db_ops)?;
    }
    Ok(DedupeRunSummary {
        duplicate_groups,
        files_linked,
    })
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

fn byte_progress(label: &str, total: u64) -> ProgressBar {
    let bar = ProgressBar::with_draw_target(Some(total), ProgressDrawTarget::stderr());
    bar.set_style(
        ProgressStyle::with_template(
            "{msg} [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})",
        )
        .unwrap()
        .progress_chars("##-"),
    );
    bar.set_message(label.to_string());
    bar
}

fn get_inode(metadata: &std::fs::Metadata) -> u64 {
    #[cfg(unix)]
    return metadata.ino();
    #[cfg(windows)]
    {
        // Stable file_index() is only available in newer Rust versions or via unstable features.
        // For now, we return 0 on Windows, which disables the inode-based optimization
        // but keeps the tool safe and functional.
        let _ = metadata;
        0
    }
}

fn print_summary(mode: &str, groups: &HashMap<Hash, Vec<PathBuf>>) {
    let duplicates = groups.values().filter(|g| g.len() > 1).count();
    println!("{mode} complete. duplicate groups: {duplicates}");
}

fn format_number(val: usize) -> String {
    let s = val.to_string();
    let chars: Vec<char> = s.chars().rev().collect();
    let mut result = Vec::new();
    for (i, c) in chars.iter().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(' ');
        }
        result.push(*c);
    }
    result.into_iter().rev().collect()
}

fn format_bytes(bytes: u64) -> String {
    if bytes == 0 {
        return "0 B".to_string();
    }
    let units = ["B", "KB", "MB", "GB", "TB", "PB"];
    let mut size = bytes as f64;
    let mut unit_idx = 0;
    while size >= 1024.0 && unit_idx < units.len() - 1 {
        size /= 1024.0;
        unit_idx += 1;
    }
    format!("{:.1} {}", size, units[unit_idx])
}

fn print_vault_status(summary: &crate::state::VaultSummary) {
    let mut vault_loc = summary.vault_location.clone();
    if let Ok(home) = std::env::var("HOME").or_else(|_| std::env::var("USERPROFILE"))
        && vault_loc.starts_with(&home)
    {
        vault_loc = vault_loc.replacen(&home, "~", 1);
    }
    let mut display_loc = vault_loc.replace('\\', "/");
    if !display_loc.ends_with('/') {
        display_loc.push('/');
    }

    println!("{:<17}: {}", "Vault location", display_loc.white().bold());
    println!(
        "{:<17}: {}",
        "Objects in vault",
        format_number(summary.objects_in_vault).white().bold()
    );
    println!(
        "{:<17}: {}",
        "Total vault size",
        format_bytes(summary.total_vault_size).green().bold()
    );
    println!(
        "{:<17}: {}",
        "Tracked paths",
        format_number(summary.tracked_paths).white().bold()
    );
    println!(
        "{:<17}: {}",
        "Estimated savings",
        format_bytes(summary.estimated_savings).green().bold()
    );
    println!(
        "{:<17}: {}",
        "Deduplication ratio",
        format!("{:.2}×", summary.deduplication_ratio)
            .yellow()
            .bold()
    );
}

fn restore_pipeline(
    path: &Path,
    state: &state::State,
    dry_run: bool,
    vault_dir: &Path,
) -> Result<()> {
    let quiet = hasher::is_quiet();
    let multi = MultiProgress::with_draw_target(ProgressDrawTarget::stderr());
    let restore_spinner = if quiet {
        ProgressBar::hidden()
    } else {
        multi.add(ProgressBar::new_spinner())
    };
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

        let inode = get_inode(&metadata);
        let size = metadata.len();

        let mut needs_restore = false;
        let mut target_hash: Option<Hash> = None;

        if inode != 0 && state.is_inode_vaulted(inode).unwrap_or(false) {
            needs_restore = true;
            if let Ok(Some(file_meta)) = state.get_file_metadata(&file_path) {
                target_hash = Some(file_meta.hash);
            }
        } else if let Ok(Some(file_meta)) = state.get_file_metadata(&file_path) {
            let vault_path = vault::shard_path(vault_dir, &file_meta.hash);
            if vault_path.exists() {
                needs_restore = true;
                target_hash = Some(file_meta.hash);
            }
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
                        let _ = vault::remove_from_vault(vault_dir, &hash);
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
