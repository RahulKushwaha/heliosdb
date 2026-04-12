use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use heliosdb_engine::memtable::MemTable;
use bytes::Bytes;

fn bench_memtable_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("memtable/insert");
    for n in [1_000usize, 10_000, 100_000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                let mt = MemTable::new();
                for i in 0..n as u64 {
                    mt.put(
                        Bytes::from(format!("key{i:08}")),
                        i,
                        Bytes::from(format!("value{i}")),
                    );
                }
                mt
            });
        });
    }
    group.finish();
}

fn bench_memtable_get(c: &mut Criterion) {
    let mt = MemTable::new();
    let n = 100_000u64;
    for i in 0..n {
        mt.put(Bytes::from(format!("key{i:08}")), i, Bytes::from(format!("v{i}")));
    }

    c.bench_function("memtable/get_random", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = format!("key{:08}", i % n);
            mt.get(key.as_bytes(), n);
            i += 1;
        });
    });
}

criterion_group!(benches, bench_memtable_insert, bench_memtable_get);
criterion_main!(benches);
