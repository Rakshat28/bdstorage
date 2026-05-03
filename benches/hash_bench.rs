use bdstorage::hasher::{full_hash, full_hash_buffered};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::io::Write;
use tempfile::NamedTempFile;

fn make_temp_file(size: usize) -> NamedTempFile {
    let mut f = NamedTempFile::new().expect("create temp file");
    // Pseudorandom-looking pattern so the OS cannot trivially short-circuit reads.
    let chunk: Vec<u8> = (0..256).map(|i| i as u8).cycle().take(size).collect();
    f.write_all(&chunk).expect("write bench data");
    f.flush().expect("flush bench data");
    f
}

fn bench_full_hash(c: &mut Criterion) {
    // Sizes chosen to cover: below threshold (buffered only), at threshold, well above.
    let sizes: &[(usize, &str)] = &[
        (512 * 1024, "512 KiB"),
        (1024 * 1024, "1 MiB"),
        (4 * 1024 * 1024, "4 MiB"),
        (16 * 1024 * 1024, "16 MiB"),
    ];

    let mut group = c.benchmark_group("full_hash");

    for &(size, label) in sizes {
        let f = make_temp_file(size);
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_with_input(BenchmarkId::new("mmap", label), &f, |b, f| {
            b.iter(|| full_hash(f.path()).expect("full_hash"));
        });

        group.bench_with_input(BenchmarkId::new("buffered", label), &f, |b, f| {
            b.iter(|| full_hash_buffered(f.path()).expect("full_hash_buffered"));
        });
    }

    group.finish();
}

criterion_group!(benches, bench_full_hash);
criterion_main!(benches);
