# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.2] - 2026-02-28

### Changed
- **Massive Performance Boost for Tiny Files:** Completely refactored the internal database architecture to use a dedicated asynchronous writer thread and transaction batching. 
- **Benchmark Milestone:** This optimization eliminated the disk I/O bottleneck for the embedded `redb` database, dropping deduplication latency on 15,000 tiny files from ~20.1 seconds down to ~211 milliseconds (nearly a 100x speedup), making `bdstorage` faster than both `rmlint` and `jdupes` in all tested scenarios.

## [0.1.1] - 2026-02-28

### Added
- Implemented metadata preservation (xattrs, permissions, timestamps) during the deduplication process (#25).

### Fixed
- Implemented atomic vault renaming to prevent master file corruption during unexpected interruptions (#29).
- Fixed a critical safety issue to ensure the master file is fully restored if a subsequent reflink operation fails (#27).
- Fixed and improved the hardlink fallback logic for filesystems that do not support CoW reflinks (#24).

### Changed
- Hardened the integration test suite to strictly validate sparse files, bit-rot simulations, and CI runner isolation (#30).