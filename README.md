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
6. [Data Locations & Storage](#-data-locations--storage)
7. [Safety Guarantees](#-safety-guarantees)
8. [License](#-license)

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
Because `bdstorage` uses a tiered sparse-hashing pipeline, it rejects large files with tiny differences almost instantly without reading the entire file.

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
     --prepare 'rm -rf ~/.bdstorage && rm -rf /tmp/bench_data/arena_tiny/test && cp -r /tmp/bench_data/arena_tiny/pristine /tmp/bench_data/arena_tiny/test' \
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

---

## Installation

### Option 1: Install via Cargo (crates.io)
```bash
cargo install bdstorage
```

### Option 2: Build from Source
```bash
git clone [https://github.com/Rakshat28/bdstorage](https://github.com/Rakshat28/bdstorage)
cd bdstorage
cargo build --release
```

---

## Usage Guide

### 1. Scan (Read-Only Analysis)
Analyze a directory to find duplicate candidates. This operation is 100% read-only and will not move files or modify your database.
```bash
bdstorage scan /path/to/directory
```

### 2. Dedupe (Write-Mode)
Execute the deduplication process. Master files are vaulted, and duplicates are replaced with reflinks.
```bash
bdstorage dedupe /path/to/directory
```

**Flags:**
* `--paranoid`: Perform a strict byte-for-byte comparison against the vaulted file before linking to guarantee 100% collision safety and protect against bit rot.
* `-n, --dry-run`: Simulate the deduplication process, printing what *would* happen without actually modifying the filesystem or database.
* `--allow-unsafe-hardlinks`: Enable hard link fallback when the filesystem does not support CoW reflinks. Hard links share the same inode, meaning all linked files will have identical metadata (timestamps, permissions). Best suited for read-only data or scenarios where metadata independence is not required.

### 3. Restore (Un-Dedupe)
Reverse the deduplication process. This breaks the shared links and restores independent, physical copies of the data back to their original locations.
```bash
bdstorage restore /path/to/directory
```
*Note: If a vaulted file's reference count drops to zero during a restore, `bdstorage` automatically prunes it to free up space (Garbage Collection).*

**Flags:**
* `-n, --dry-run`: Simulate the restoration process without modifying the filesystem.

---

## Data Locations & Storage

Your data never leaves your machine. `bdstorage` automatically provisions the following directories in your home folder:

* **State DB:** `~/.bdstorage/state.redb`
* **CAS Vault:** `~/.bdstorage/store/`

To perform a completely clean reset of the engine:
```bash
rm -f ~/.bdstorage/state.redb
rm -rf ~/.bdstorage/store/
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
