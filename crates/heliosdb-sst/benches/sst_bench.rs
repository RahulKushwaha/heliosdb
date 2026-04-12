use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use heliosdb_sst::{
    block::{BlockBuilder, CompressionType},
    bloom::BloomBuilder,
    builder::SstBuilder,
};
use std::io::Cursor;

fn bench_block_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("block/encode");
    for n in [100usize, 1_000, 10_000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                let mut builder = BlockBuilder::new(CompressionType::None);
                for i in 0..n {
                    builder.add(format!("key{i:08}").as_bytes(), b"value");
                }
                builder.finish().unwrap()
            });
        });
    }
    group.finish();
}

fn bench_bloom_build(c: &mut Criterion) {
    let mut group = c.benchmark_group("bloom/build");
    for n in [1_000usize, 100_000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                let mut bloom = BloomBuilder::new(n);
                for i in 0..n {
                    bloom.add(format!("key{i}").as_bytes());
                }
                bloom.finish()
            });
        });
    }
    group.finish();
}

fn bench_sst_build(c: &mut Criterion) {
    let mut group = c.benchmark_group("sst/build");
    for n in [1_000usize, 10_000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                let buf = Vec::new();
                let mut builder = SstBuilder::new(Cursor::new(buf), n, CompressionType::None);
                for i in 0..n {
                    builder.add(format!("key{i:08}").as_bytes(), b"value").unwrap();
                }
                builder.finish().unwrap()
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_block_encode, bench_bloom_build, bench_sst_build);
criterion_main!(benches);
