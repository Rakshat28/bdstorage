# Contributing to bdstorage

`bdstorage` is a speed-first, local file deduplication engine designed to maximize storage efficiency using tiered BLAKE3 hashing and Copy-on-Write (CoW) reflinks. We welcome contributions from the community—whether it's reporting bugs, proposing new features, or submitting pull requests.

This document provides guidelines and instructions for contributing to this project.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [How Can I Contribute?](#how-can-i-contribute)
  - [Reporting Bugs](#reporting-bugs)
  - [Suggesting Enhancements](#suggesting-enhancements)
  - [Submitting Pull Requests](#submitting-pull-requests)
- [Local Development Setup](#local-development-setup)
- [Development Workflow](#development-workflow)
- [Coding Guidelines](#coding-guidelines)
- [Architecture Overview](#architecture-overview)

## Code of Conduct

By participating in this project, you agree to abide by our Code of Conduct. We expect all contributors to maintain a respectful and inclusive environment for everyone. 

## How Can I Contribute?

### Reporting Bugs

If you find a bug, please create an issue on our [GitHub repository](https://github.com/Rakshat28/bdstorage/issues). 
When reporting a bug, please include:
- Your operating system and version.
- The filesystem you are using (e.g., Btrfs, ext4, XFS).
- The exact command you ran.
- The expected behavior vs. the actual behavior.
- Any relevant logs, panic backtraces, or error messages.

### Suggesting Enhancements

Have an idea for a new feature or a way to optimize the hashing pipeline? We'd love to hear it! 
Open an issue and use the "Enhancement" label if possible. Please provide a clear description of the feature, why it's needed, and how it aligns with `bdstorage`'s speed-first philosophy.

### Submitting Pull Requests

1. Fork the repository and create your branch from `main`.
2. Name your branch descriptively (e.g., `feat/add-new-hasher` or `fix/vault-transfer-bug`).
3. If you've added code that should be tested, add tests.
4. Ensure the test suite passes.
5. Format your code using `cargo fmt` and check for lints using `cargo clippy`.
6. Issue that pull request!

## Local Development Setup

To build and test `bdstorage` locally, you will need the standard Rust toolchain.

1. **Install Rust:** If you haven't already, install Rust using [rustup](https://rustup.rs/).
2. **Clone the repository:**
   ```bash
   git clone [https://github.com/Rakshat28/bdstorage.git](https://github.com/Rakshat28/bdstorage.git)
   cd bdstorage
   ```
3. **Build the project:**
   ```bash
   cargo build
   ```
4. **Run the project:**
   ```bash
   cargo run -- --help
   ```

*Note: Since `bdstorage` uses Linux-specific APIs (like `fiemap` ioctls) for sparse file optimization, developing on a Linux environment is highly recommended.*

## Development Workflow

Before submitting a Pull Request, please ensure your changes pass the standard Rust quality checks:

1. **Formatting:** We follow standard Rust formatting rules.
   ```bash
   cargo fmt --all
   ```
2. **Linting:** Ensure there are no Clippy warnings.
   ```bash
   cargo clippy --all-targets --all-features -- -D warnings
   ```
3. **Testing:** Run the test suite to ensure no existing functionality is broken.
   ```bash
   cargo test
   ```

## Coding Guidelines

- **Error Handling:** Use the `anyhow` crate for error propagation. Always add descriptive context to errors using `.with_context(|| "description")`.
- **Performance:** `bdstorage` is designed to be extremely fast. Be mindful of disk I/O, memory allocations, and expensive system calls. Avoid reading full file contents unless absolutely necessary (rely on the tiered sparse-hashing pipeline).
- **Atomicity:** Any filesystem operations (moving, renaming, creating vault entries) must be atomic. Do not leave partial files in the `.imprint` store. 
- **Safety:** Minimize the use of `unsafe` code blocks. When interacting with C APIs (like `ioctl`), heavily document why the `unsafe` block is required and why it is safe in that context.

## Architecture Overview

If you are new to the codebase, here is a quick primer on how things are structured in `src/`:

- `main.rs`: The CLI entry point, argument parsing via `clap`, and concurrent coordination.
- `scanner.rs`: Logic for walking directories and initially grouping files by byte size.
- `hasher.rs`: Implementation of the tiered hashing logic (sparse hashing vs. full BLAKE3 hashing).
- `dedupe.rs`: Core logic for reflinking, hard linking, and restoring files.
- `vault.rs`: Manages the local Content-Addressable Storage (CAS) hidden in `~/.imprint/store`.
- `state.rs`: The embedded `redb` database integration for tracking file metadata and refcounts.

---

Thank you for contributing to `bdstorage`! Your efforts help make this tool faster, safer, and better for everyone.
