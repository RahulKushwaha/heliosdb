# Running the Benchmarks

## Prerequisites

```bash
# Install mdbook (only needed for the book)
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
