# bdstorage DeDuplication

**A speed-first, local file deduplication engine designed to maximize storage efficiency using tiered BLAKE3 hashing and Copy-on-Write (CoW) reflinks.**

`bdstorage` scans a target directory, detects identical files through a highly optimized pipeline, and replaces duplicates with lightweight links back to a centralized vault. It is built in Rust and tailored for modern Linux filesystems.

---

## Table of Contents
1. [Why bdstorage?](#-why-bdstorage)
2. [How It Works (Architecture)](#-how-it-works-architecture)
3. [System Requirements](#-system-requirements)
4. [Installation](#-installation)
5. [Usage Guide](#-usage-guide)
   - [Quick start](#quick-start)
   - [Typical workflow](#typical-workflow)
   - [What one `dedupe` run does](#what-one-dedupe-run-does)
   - [Commands and flags](#commands-and-flags)
6. [Background Daemon (Linux Only)](#background-daemon-linux-only)
   - [Overview](#overview)
   - [Installation & Setup](#installation--setup)
   - [Configuring the Scan Interval](#configuring-the-scan-interval)
   - [Managing the Daemon](#managing-the-daemon)
   - [How to Verify it is Working](#how-to-verify-it-is-working)
   - [Known Limitations](#known-limitations)
7. [Data Locations & Storage](#-data-locations--storage)
8. [Safety Guarantees](#-safety-guarantees)
9. [License](#-license)

---

## Why bdstorage?

Traditional deduplication tools often thrash your disk by reading every single byte of every file. `bdstorage` takes a smarter, speed-first approach to minimize I/O overhead.

It employs a **Tiered Hashing Pipeline**:
1. **Size Grouping (Zero I/O):** Files are grouped by exact byte size. Unique sizes are immediately discarded from the deduplication pool.
2. **Sparse Hashing (Minimal I/O):** For files larger than 12KB, the engine reads a small 12KB sample (4KB from the start, middle, and end) to quickly eliminate files that share the same size but have different contents. On Linux, it leverages `fiemap` ioctls to handle sparse files intelligently.
3. **Full BLAKE3 Hashing (High Throughput):** Only files that pass the sparse hash check undergo a full BLAKE3 cryptographic hash using a high-performance 128KB buffer to confirm identical content.

---
## Benchmarks vs. Competitors

`bdstorage` was benchmarked against `jdupes` and `rmlint` using `hyperfine`. Tests were run on an ext4 filesystem with a cleared OS cache and a fresh state database before every run.

**Arena 1: Massive Sparse Files (100MB files, 1-byte difference)**
Because `bdstorage` uses a tiered sparse-hashing pipeline, it rejects large files with no differences almost instantly without reading the entire file.

| Command | Mean [ms] | Min [ms] | Max [ms] | Relative |
|:---|---:|---:|---:|---:|
| `bdstorage dedupe` | **87.0 ± 3.5** | 81.8 | 93.0 | **1.00** |
| `jdupes -r` | 101.5 ± 5.0 | 96.8 | 115.0 | 1.17 ± 0.07 |
| `rmlint` | 291.4 ± 28.4 | 265.0 | 345.9 | 3.35 ± 0.35 |

**Arena 2: Deep Trees of Tiny Files (15,000 files across 100 directories)**
Thanks to asynchronous database transaction batching and a multi-threaded `crossbeam` architecture, `bdstorage` efficiently manages massive source code and log directories while maintaining a persistent, highly-safe CAS vault.

| Command | Mean [ms] | Min [ms] | Max [ms] | Relative |
|:---|---:|---:|---:|---:|
| `bdstorage dedupe` | **211.9 ± 32.9** | 164.5 | 262.6 | **1.00** |
| `rmlint` | 292.4 ± 22.4 | 280.9 | 355.5 | 1.38 ± 0.24 |
| `jdupes -r` | 1454.4 ± 5.6 | 1446.6 | 1461.7 | 6.86 ± 1.07 |

---

### Reproducing the Benchmarks

Transparency is critical. You can reproduce these exact numbers on your own machine using the scripts provided in the repository.

1. Navigate to the benchmarks directory:
   ```bash
   cd benchmarks
   ```
2. Generate the exact testing arenas (Sparse Files and Deep Trees):
   ```bash
   ./setup_bench.sh
   ```
3. Run the `hyperfine` race (Example for Arena 3):
   ```bash
   hyperfine \
     --warmup 1 \
     --prepare 'rm -rf ~/.imprint && rm -rf /tmp/bench_data/arena_tiny/test && cp -r /tmp/bench_data/arena_tiny/pristine /tmp/bench_data/arena_tiny/test' \
     '../target/release/bdstorage dedupe /tmp/bench_data/arena_tiny/test' \
     'rmlint /tmp/bench_data/arena_tiny/test' \
     'jdupes -r /tmp/bench_data/arena_tiny/test'
   ```
*(Note: Ensure you have `hyperfine`, `rmlint`, and `jdupes` installed on your system before running).*

---

## How It Works (Architecture)

When identical files are confirmed, `bdstorage` uses a **Content-Addressable Storage (CAS) Vault**.

1. **Vaulting:** The first instance of a file (the "master") is moved into a hidden local vault. It is renamed to its BLAKE3 hash.
2. **Linking:** `bdstorage` replaces the original file and any subsequent duplicates with a link pointing to the vaulted master.
    * **Primary Strategy (Reflink - Strict Default):** Creates a Copy-on-Write (CoW) reflink. This is instantaneous, shares the underlying disk extents, and preserves data independence. Reflinks preserve each file's individual metadata (permissions, modification times, extended attributes). If the filesystem does not support reflinks, files are skipped by default.
    * **Alternative Strategy (Hard Link):** Available via the `--allow-unsafe-hardlinks` flag. Hard links share the same inode, which means all linked files share the same metadata (timestamps, permissions). This is suitable for read-only archives or when metadata independence is not required. Note that modifying any hard-linked file will affect all linked copies since they share the same underlying inode.
3. **State Tracking:** An embedded, low-latency `redb` database tracks file metadata, vault index, and reference counts to ensure nothing is accidentally deleted.
4. **Metadata Preservation:** When using reflinks, `bdstorage` automatically preserves each file's original permissions, modification times, and extended attributes, ensuring deduplication is completely transparent to applications.

---

## System Requirements

* **Operating System:** Linux (Required for `fiemap` ioctl sparse file optimizations).
* **Filesystem:** For maximum performance and safety, a filesystem that supports **reflinks** (e.g., Btrfs, XFS) is strongly recommended.
* **Rust:** Latest stable toolchain (if building from source).

### ZFS Support
`bdstorage` supports **OpenZFS 2.2+** native block-cloning. 

* **OpenZFS 2.2 and newer:** Supports native CoW reflinks. `bdstorage` will automatically use `LinkType::Reflink` for instant, space-efficient deduplication while preserving independent file metadata (timestamps, permissions).
* **OpenZFS < 2.2:** Does not support explicit block-cloning. By default, `bdstorage` will skip these files and report that reflinks are not supported.
* **Workaround for older ZFS:** If you are on an older version of ZFS and want to save space, use the `--allow-unsafe-hardlinks` flag. **Warning:** This uses hard links, meaning all duplicates will share the same inode and metadata. Modifying one copy will affect all others.

---

## Installation

### Option 1: Install via Cargo (crates.io)
```bash
cargo install bdstorage
```

### Option 2: Build from Source
```bash
git clone https://github.com/Rakshat28/bdstorage.git
cd bdstorage
cargo build --release
```

---

## Usage Guide

### Quick start

1. **Install** (see [Installation](#-installation)). From a source build, the binary is `target/release/bdstorage` (add it to your `PATH` or call it by full path).
2. **Pick a directory tree** on a filesystem that supports **reflinks** if you want the default behavior (see [System Requirements](#-system-requirements)). On ext4 without reflinks, files are skipped unless you use `--allow-unsafe-hardlinks` (understand the metadata implications first).
3. **Preview**, then **apply**:
   ```bash
   bdstorage dedupe /path/to/tree -n    # dry-run: no writes
   bdstorage dedupe /path/to/tree       # real run
   ```
4. **State and vault** are created under **`$HOME/.imprint/`** on first use (overridable via `--vault-dir` or `$BDSTORAGE_VAULT`; see [Data Locations & Storage](#-data-locations--storage)).

Use **`bdstorage --help`** and **`bdstorage <subcommand> --help`** for the full CLI.

### Typical workflow

| Step | Command | What you get |
|:---|:---|:---|
| 1  | `bdstorage scan /path/to/tree` | Same walk + hash + DB indexing as dedupe, and prints duplicate **group** count; does **not** vault files or create links. |
| 2  | `bdstorage dedupe /path/to/tree -n` | Same logic as a real dedupe, but only prints what **would** happen. |
| 3 | `bdstorage dedupe /path/to/tree` | Vaults one copy per duplicate group and replaces the rest with reflinks (or hard links if allowed). |
| 4 (optional) | `bdstorage daemon run /path/to/tree --interval-secs 3600` | Repeats step 3 on an interval; see [Background Daemon](#background-daemon-linux-only). |
| If you need originals back | `bdstorage restore /path/to/tree` | Copies data back from the vault and breaks links; see restore flags below. |

Run **`restore`** when you want independent file copies again (for example before migrating data off the machine or when you no longer want shared extents).

### What one `dedupe` run does

End to end, a single **`bdstorage dedupe <path>`** does the following:

1. **Walk** the tree and collect regular files (scratch names like `*.imprint_tmp` are ignored).
2. **Group by size** — files whose size appears only once cannot have a same-size duplicate, so they are dropped from further work without extra reads.
3. **Sparse sample hash** — for larger files, read small samples (start / middle / end) so different content is often rejected without a full read.
4. **Full BLAKE3** — remaining candidates get a full-file hash; matching hashes mean identical content for practical purposes.
5. **Vault** — for each group with two or more paths, one file becomes the **master** in **`~/.imprint/store/`**, addressed by hash (see [Architecture](#-how-it-works-architecture) for reflink vs hard link).
6. **Replace duplicates** — other paths in the group are replaced by links to the vaulted master; the embedded **redb** database records paths, hashes, refcounts, and vaulted inode markers so later runs and **`restore`** stay consistent.

Interrupted runs are designed so you do not end up with half-linked files without the vault copy in place; see [Safety Guarantees](#-safety-guarantees).

### Commands and flags

**Scan** (read-only analysis):

```bash
bdstorage scan /path/to/directory
```

**Dedupe** (writes vault + links):

```bash
bdstorage dedupe /path/to/directory
```

| Flag | Meaning |
|:---|:---|
| `--paranoid` | Before linking, compare bytes against the vaulted master (extra safety / bit-rot detection). |
| `-n`, `--dry-run` | Print actions only; no filesystem or DB changes for real dedupe. |
| `--allow-unsafe-hardlinks` | If reflinks are unsupported, use hard links instead of skipping (shared inode and metadata). |

**Restore** (copy back from vault, unlink deduped files):

```bash
bdstorage restore /path/to/directory
```

| Flag | Meaning |
|:---|:---|
| `-n`, `--dry-run` | Show what would be restored without writing. |

When a vault object’s refcount hits zero during restore, it is **removed** (garbage collection).

## Background Daemon (Linux Only)

### Overview

`bdstorage` can run continuously in the background using systemd to automatically deduplicate a specific folder (and all subfolders) at a set time interval.

**Crucial Note:** installation uses `sudo` because systemd unit files are system-level, but `bdstorage` dynamically detects your account and configures the daemon to run with your normal user permissions. The daemon uses your normal `~/.imprint/` vault and state database, not a root vault.

### Installation & Setup

**Step 1: Install the service**

```bash
sudo bdstorage daemon install --target /path/to/watch --interval-secs 60
```

**Step 2: Note about Filesystems (IMPORTANT)**

> **IMPORTANT:** If your target is on a standard filesystem like ext4 (no CoW reflinks), you must add `--allow-unsafe-hardlinks` to the install command. If you do not, the daemon intentionally skips deduplication on unsupported filesystems to protect your files.

```bash
sudo bdstorage daemon install --target /path/to/watch --interval-secs 60 
```

**Step 3: Enable and Start**

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now bdstorage-dedupe.service
```

### Configuring the Scan Interval

Use `--interval-secs` to control how often the daemon wakes up and runs deduplication.

**Short Intervals (e.g., 5 to 30 seconds)**

- **Pros:** Near-instant deduplication. Files are linked and space is recovered almost immediately after you download or copy them.
- **Cons:** Higher idle CPU usage and more frequent disk wake-ups, which can drain laptop batteries faster.

**Long Intervals (e.g., 3600 seconds / 1 Hour)**

- **Pros:** Extremely lightweight. Zero noticeable impact on system performance or battery life.
- **Cons:** Temporary duplicate files will sit on your hard drive taking up wasted space until the hour is up and the next scan triggers.

### Managing the Daemon

**Check Status**

```bash
systemctl status bdstorage-dedupe.service
```

**Watch Live Logs**

```bash
journalctl -u bdstorage-dedupe.service -f
```

**Pause the Daemon**

```bash
sudo systemctl stop bdstorage-dedupe.service
```

**Permanently Disable & Stop**

```bash
sudo systemctl disable --now bdstorage-dedupe.service
```

### How to Verify it is Working

Run `ls -l` inside your watched target folder.

Check the **link count** column (the number after permissions):

- `1` means the file has not been deduplicated yet.
- `2` (or more) means the daemon successfully linked that file to the vault.

### Known Limitations

- This daemon flow is driven by systemd, so it is currently Linux-only.
- The daemon only operates on the specific `--target` directory you configured, leaving the rest of your system untouched.

---

## Data Locations & Storage

Your data never leaves your machine. By default, `bdstorage` uses **`$HOME/.imprint/`**:

* **State DB:** `~/.imprint/state.redb`
* **CAS Vault:** `~/.imprint/store/`

### Overriding the vault location

You can point the vault and state DB at a different directory in two ways (listed in priority order):

| Method | Example |
|:---|:---|
| CLI flag | `bdstorage --vault-dir /mnt/bigdisk dedupe ~/Downloads` |
| Environment variable | `export BDSTORAGE_VAULT=/mnt/bigdisk` |

Both `state.redb` and `store/` will be placed inside the chosen directory.

To perform a completely clean reset of the engine:
```bash
rm -f ~/.imprint/state.redb
rm -rf ~/.imprint/store/
```

---

## Safety Guarantees

We take your data seriously. `bdstorage` is designed with the following invariants:
* **No Premature Deletion:** Original data is never removed until a verified copy has been successfully written to the CAS vault.
* **Verification First:** Hash verification is consistently performed before linking.
* **Atomic Failures:** If the process is interrupted, partially processed files are left completely untouched.
* **Link Safety:** Reflinks and hard links are only created after a successful vault storage operation.

---

## License

This project is open-source and distributed under the **Apache License 2.0**.
