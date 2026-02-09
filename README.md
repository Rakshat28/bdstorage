# Imprint Deduplication Engine

Imprint is a speed-first, local file deduplication CLI. It scans a target directory, detects identical files using a tiered hashing pipeline, and replaces duplicates with reflinks (primary) or hard links (fallback). Identical content is stored in a content-addressable storage (CAS) vault backed by a small embedded database.

---

## Key Concepts

- **Tiered pipeline** to minimize I/O:
  1. **Size grouping** (zero-I/O)
  2. **Sparse hashing** (12KB sample: first/middle/last)
  3. **Full hashing** (BLAKE3 with 128KB buffer)

- **CAS Vault**: Files are stored by content hash using a sharded directory layout.

- **Linking strategy**:
  - **Reflink** first (fast, CoW, same FS).
  - **Hard link** fallback (same device).

- **State DB**: Tracks file metadata and content references.

---

## Requirements

- **Linux**
- **Rust** (latest stable)
- **Filesystem with reflink support** for best results (e.g., Btrfs, XFS). Hard links are used when reflinks aren’t available.

---

## Local Setup

1) **Clone**
```bash
git clone https://github.com/Rakshat28/imprint
cd imprint
```

2) **Install Rust**
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.cargo/env
```

3) **Build**
```bash
cargo build
```

---

## Usage

### 1) Scan
Runs the tiered pipeline to find duplicate candidates.

```bash
cargo run -- scan /path/to/dir
```

### 2) Dedupe
Moves the master copy to the vault and links duplicates back.

```bash
cargo run -- dedupe /path/to/dir
```

---

## Data Locations

- **State DB**: `~/.imprint/state.redb`
- **CAS Vault**: `~/.imprint/store/`

The DB and vault are created automatically on first run.

---

## How Deduplication Works (High Level)

1. **Walk** the directory in parallel (jwalk).
2. **Group by size**; unique sizes are discarded.
3. **Sparse hash** (12KB sample) to filter further.
4. **Full hash** to confirm identical content.
5. **Vault**:
   - If the hash doesn’t exist, move the file into the vault.
   - If it exists, it is the master copy.
6. **Link**:
   - Reflink from vault to original location.
   - If reflink fails, hard link instead.

---

## Reset / Clean State

To remove the DB and vault:

```bash
rm -f ~/.imprint/state.redb
rm -rf ~/.imprint/store/
```


## Safety Guarantees

- Original data is never deleted until a verified copy exists in the vault.
- Hash verification is always performed before linking.
- If the process is interrupted, partially processed files are left untouched.
- Reflinks/hardlinks are only created after successful vault storage.
