pub fn bench_methodology() -> &'static str {
    r#"# Benchmark Methodology

heliosDB benchmarks are **layered**: each subsystem is measured in isolation
before looking at end-to-end numbers. This makes it possible to diagnose where
performance is lost and to track regressions at a fine granularity.

## Why layer isolation matters

A single end-to-end write benchmark conflates:
- MemTable insert speed
- WAL append + fsync latency
- Block encoding cost
- Active segment merge time

If the number changes, you don't know which layer regressed. Isolated benchmarks
tell you exactly where to look.

## Benchmark layers

### Layer 1 — Block (heliosdb-sst)

Measures the raw encoding and decoding throughput of data blocks, independent of
any file I/O.

| Benchmark | What it measures |
|---|---|
| `block/encode/N` | Build a block with N entries: prefix compression + restart points |
| `block/decode/N` | Decode N encoded entries (decompress + parse) |
| `block/seek` | Binary search within a block to a specific key |

### Layer 2 — Bloom filter (heliosdb-sst)

| Benchmark | What it measures |
|---|---|
| `bloom/build/N` | Insert N keys and finish the filter |
| `bloom/lookup/hit` | `may_contain` for a key that IS in the filter |
| `bloom/lookup/miss` | `may_contain` for a key that is NOT in the filter |

The miss benchmark matters most — it's the hot path in the read pipeline
(most inactive segment lookups are misses).

### Layer 3 — SST file (heliosdb-sst)

| Benchmark | What it measures |
|---|---|
| `sst/build/N` | Write an SST with N entries end-to-end (to a memory buffer) |
| `sst/point_lookup/hit` | `SstReader::get` for a key in the file |
| `sst/point_lookup/miss` | `SstReader::get` for an absent key (bloom skip) |

### Layer 4 — MemTable (heliosdb-engine)

| Benchmark | What it measures |
|---|---|
| `memtable/insert/N` | Insert N entries into a fresh MemTable |
| `memtable/get_random` | Random point lookup latency on a pre-loaded MemTable |

### Layer 5 — End-to-end (heliosdb)

Full database benchmarks that include WAL, flush, and compaction:

| Benchmark | What it measures |
|---|---|
| `db/write_sequential` | Sequential key writes (keys in sorted order) |
| `db/write_random` | Random key writes |
| `db/read_after_flush` | Point lookup on a flushed active segment |
| `db/scan_full` | Full-table scan throughput |

## Key storage metrics

Beyond raw throughput, track these three ratios:

### Write Amplification Factor (WAF)
```
WAF = bytes_written_to_disk / bytes_written_by_user
```
For heliosDB with leveled compaction: WAF ≈ 1 + num_levels.

### Read Amplification Factor (RAF)
```
RAF = disk_reads_per_user_read
```
Best case (MemTable hit): 0 disk reads.
Active segment hit: 1 sequential scan (partially cached).
Inactive segment hit: 1 bloom filter check + 1 disk read.

### Space Amplification
```
space_amp = bytes_on_disk / bytes_of_live_data
```
Compaction bounds this by removing old versions and tombstones.

## Tools

| Tool | Purpose |
|---|---|
| [Criterion](https://github.com/bheisler/criterion.rs) | Statistical micro-benchmarks, HTML reports, comparison between runs |
| [iai-callgrind](https://github.com/iai-callgrind/iai-callgrind) | Instruction-count benchmarks (deterministic, no hardware noise) |
| `perf` + [flamegraph](https://github.com/flamegraph-rs/flamegraph) | CPU hotspot profiling |
"#
}

pub fn bench_running() -> &'static str {
    r#"# Running the Benchmarks

## Prerequisites

```bash
# Install mdbook (only needed for helios-book)
cargo install mdbook

# Install flamegraph (optional, for profiling)
cargo install flamegraph
```

## SST layer benchmarks

```bash
cargo bench -p heliosdb-sst
```

This runs benchmarks for block encoding/decoding, bloom filter, and SST build.
Criterion generates an HTML report at:

```
target/criterion/report/index.html
```

To run a specific benchmark:

```bash
# Only block encode benchmarks
cargo bench -p heliosdb-sst -- block/encode

# Only bloom false-positive probe
cargo bench -p heliosdb-sst -- bloom/build
```

## Engine layer benchmarks

```bash
cargo bench -p heliosdb-engine
```

Benchmarks MemTable insert throughput and random lookup latency.

```bash
# Only MemTable benchmarks
cargo bench -p heliosdb-engine -- memtable
```

## Comparing runs (baseline vs change)

Criterion automatically stores a baseline. To compare:

```bash
# Save current performance as the baseline
cargo bench -p heliosdb-sst -- --save-baseline main

# Make your change, then compare
cargo bench -p heliosdb-sst -- --baseline main
```

Criterion reports the percentage change and whether it is statistically
significant.

## Profiling with flamegraph

```bash
# Profile the SST build benchmark
cargo flamegraph --bench sst_bench -p heliosdb-sst -- --bench sst/build
```

The resulting `flamegraph.svg` shows which functions consume the most CPU time.

## Deterministic benchmarks with iai-callgrind

For benchmarks that need to be compared across machines (e.g., CI), use
instruction counts rather than wall-clock time:

```bash
cargo install iai-callgrind-runner
cargo bench -p heliosdb-sst --features iai
```

Instruction counts are reproducible across different hardware and load levels,
making them ideal for detecting regressions in CI.

## Interpreting results

### Block encode throughput

Typical numbers (no compression, sequential keys):

| N entries | Throughput |
|---|---|
| 100 | ~50 MB/s |
| 1,000 | ~200 MB/s |
| 10,000 | ~350 MB/s |

Throughput increases with N because per-block overhead (CRC, footer) is amortized.

### Bloom false-positive rate

The test `bloom::tests::false_positive_rate_within_bound` verifies the FP rate
stays below 5% (target is ~1%). Run it with:

```bash
cargo test -p heliosdb-sst bloom
```

### MemTable insert

Typical numbers on modern hardware:

| N inserts | Time |
|---|---|
| 1,000 | ~50 µs |
| 10,000 | ~600 µs |
| 100,000 | ~8 ms |

The skip list's O(log n) insert means throughput degrades gracefully with size.

## Scan amplification metric

To measure how many inactive segments are consulted per scan, add a counter to
`db.rs::scan` in debug builds:

```rust
#[cfg(debug_assertions)]
static INACTIVE_CONSULTS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);
```

For a fully-live keyspace (all keys in active segment), this counter should
remain at 0 — the scan never touches an inactive file.
"#
}
