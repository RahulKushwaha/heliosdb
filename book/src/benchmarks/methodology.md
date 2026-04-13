# Benchmark Methodology

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
