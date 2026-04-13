use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use heliosdb_engine::memtable::{BTreeMemTable, MemTable, MemTableSet, SkipListMemTable};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn key(i: u64) -> Bytes  { Bytes::from(format!("key{i:08}")) }
fn val(i: u64) -> Bytes  { Bytes::from(format!("val{i:08}")) }

/// Build a MemTableSet with `n_immutable` full immutable slabs + one
/// partially-filled active.  `slab_size` entries per slab.
fn preloaded<M: MemTable>(
    slab_size: u64,
    n_immutable: usize,
) -> MemTableSet<M> {
    // large write_buffer_size so we control rotation manually
    let mut set: MemTableSet<M> = MemTableSet::new(usize::MAX, n_immutable + 1);
    let mut seq = 0u64;

    for _ in 0..n_immutable {
        for _ in 0..slab_size {
            set.put(key(seq), seq, val(seq));
            seq += 1;
        }
        set.rotate();
    }
    // partial active
    for _ in 0..slab_size / 2 {
        set.put(key(seq), seq, val(seq));
        seq += 1;
    }
    set
}

// ---------------------------------------------------------------------------
// put: writing into the active slot (no rotation)
// ---------------------------------------------------------------------------

fn bench_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("memtable_set/put");

    for &n in &[1_000usize, 10_000, 100_000] {
        group.throughput(Throughput::Elements(n as u64));

        group.bench_with_input(BenchmarkId::new("skiplist", n), &n, |b, &n| {
            b.iter(|| {
                let mut set: MemTableSet<SkipListMemTable> =
                    MemTableSet::new(usize::MAX, 4);
                for i in 0..n as u64 { set.put(key(i), i, val(i)); }
                set
            });
        });

        group.bench_with_input(BenchmarkId::new("btree", n), &n, |b, &n| {
            b.iter(|| {
                let mut set: MemTableSet<BTreeMemTable> =
                    MemTableSet::new(usize::MAX, 4);
                for i in 0..n as u64 { set.put(key(i), i, val(i)); }
                set
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// put with rotation: crosses the rotate boundary every `slab` entries
// ---------------------------------------------------------------------------

fn bench_put_with_rotation(c: &mut Criterion) {
    let mut group = c.benchmark_group("memtable_set/put_with_rotation");
    let slab  = 10_000u64;
    let total = 100_000usize;
    group.throughput(Throughput::Elements(total as u64));

    group.bench_function("skiplist", |b| {
        b.iter(|| {
            let mut set: MemTableSet<SkipListMemTable> =
                MemTableSet::new((slab as usize) * 100, 16); // big enough, rotate manually
            for i in 0..total as u64 {
                set.put(key(i), i, val(i));
                if (i + 1) % slab == 0 { set.rotate(); }
            }
            set
        });
    });

    group.bench_function("btree", |b| {
        b.iter(|| {
            let mut set: MemTableSet<BTreeMemTable> =
                MemTableSet::new((slab as usize) * 100, 16);
            for i in 0..total as u64 {
                set.put(key(i), i, val(i));
                if (i + 1) % slab == 0 { set.rotate(); }
            }
            set
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// rotate: cost of sealing the active slab
// ---------------------------------------------------------------------------

fn bench_rotate(c: &mut Criterion) {
    let mut group = c.benchmark_group("memtable_set/rotate");

    for &slab in &[1_000u64, 10_000, 100_000] {
        group.bench_with_input(BenchmarkId::new("skiplist", slab), &slab, |b, &slab| {
            b.iter(|| {
                let mut set: MemTableSet<SkipListMemTable> =
                    MemTableSet::new(usize::MAX, 256);
                for i in 0..slab { set.put(key(i), i, val(i)); }
                set.rotate();
                set
            });
        });

        group.bench_with_input(BenchmarkId::new("btree", slab), &slab, |b, &slab| {
            b.iter(|| {
                let mut set: MemTableSet<BTreeMemTable> =
                    MemTableSet::new(usize::MAX, 256);
                for i in 0..slab { set.put(key(i), i, val(i)); }
                set.rotate();
                set
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// get: hit in active (no immutables checked)
// ---------------------------------------------------------------------------

fn bench_get_active_hit(c: &mut Criterion) {
    let mut group = c.benchmark_group("memtable_set/get/active_hit");
    let n = 100_000u64;

    let sl: MemTableSet<SkipListMemTable> = preloaded(n, 0);
    group.bench_function("skiplist", |b| {
        let mut i = 0u64;
        b.iter(|| { let r = sl.get(&key(i % n), n); i += 1; r });
    });

    let bt: MemTableSet<BTreeMemTable> = preloaded(n, 0);
    group.bench_function("btree", |b| {
        let mut i = 0u64;
        b.iter(|| { let r = bt.get(&key(i % n), n); i += 1; r });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// get: hit in the deepest (oldest) immutable — worst-case lookup
// ---------------------------------------------------------------------------

fn bench_get_immutable_hit(c: &mut Criterion) {
    let mut group = c.benchmark_group("memtable_set/get/immutable_hit");

    for &n_imm in &[1usize, 2, 4] {
        let slab = 10_000u64;
        // Keys 0..slab are in the oldest (deepest) immutable
        let sl: MemTableSet<SkipListMemTable> = preloaded(slab, n_imm as usize);
        group.bench_with_input(BenchmarkId::new("skiplist", n_imm), &n_imm, |b, _| {
            let mut i = 0u64;
            b.iter(|| { let r = sl.get(&key(i % slab), slab * (n_imm as u64 + 1)); i += 1; r });
        });

        let bt: MemTableSet<BTreeMemTable> = preloaded(slab, n_imm as usize);
        group.bench_with_input(BenchmarkId::new("btree", n_imm), &n_imm, |b, _| {
            let mut i = 0u64;
            b.iter(|| { let r = bt.get(&key(i % slab), slab * (n_imm as u64 + 1)); i += 1; r });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// get: miss — key absent from all layers
// ---------------------------------------------------------------------------

fn bench_get_miss(c: &mut Criterion) {
    let mut group = c.benchmark_group("memtable_set/get/miss");
    let slab:  u64   = 10_000;
    let n_imm: usize = 2;
    let total = slab * (n_imm as u64 + 1);

    let sl: MemTableSet<SkipListMemTable> = preloaded(slab, n_imm);
    group.bench_function("skiplist", |b| {
        let mut i = 0u64;
        // keys >= total were never inserted
        b.iter(|| { let r = sl.get(&key(total + i), total * 2); i += 1; r });
    });

    let bt: MemTableSet<BTreeMemTable> = preloaded(slab, n_imm);
    group.bench_function("btree", |b| {
        let mut i = 0u64;
        b.iter(|| { let r = bt.get(&key(total + i), total * 2); i += 1; r });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// iter_all_by_priority: full scan across all layers (the scan/flush path)
// ---------------------------------------------------------------------------

fn bench_iter_all(c: &mut Criterion) {
    let mut group = c.benchmark_group("memtable_set/iter_all");

    for &n_imm in &[0usize, 1, 2] {
        let slab  = 10_000u64;
        let total = slab * (n_imm as u64 + 1);
        group.throughput(Throughput::Elements(total));

        let sl: MemTableSet<SkipListMemTable> = preloaded(slab, n_imm);
        group.bench_with_input(BenchmarkId::new("skiplist", n_imm), &n_imm, |b, _| {
            b.iter(|| sl.iter_all_by_priority().collect::<Vec<_>>());
        });

        let bt: MemTableSet<BTreeMemTable> = preloaded(slab, n_imm);
        group.bench_with_input(BenchmarkId::new("btree", n_imm), &n_imm, |b, _| {
            b.iter(|| bt.iter_all_by_priority().collect::<Vec<_>>());
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------

criterion_group!(
    benches,
    bench_put,
    bench_put_with_rotation,
    bench_rotate,
    bench_get_active_hit,
    bench_get_immutable_hit,
    bench_get_miss,
    bench_iter_all,
);
criterion_main!(benches);
