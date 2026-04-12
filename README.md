# heliosDB

A production-grade embedded key-value storage engine written in Rust, inspired by [Google Ressi](https://cloud.google.com/blog/products/databases/spanner-modern-columnar-storage-engine) — the columnar LSM-based file format that powers Google Spanner.

## Design

heliosDB adapts Ressi's core ideas to a general-purpose key-value store:

### Active / Inactive Segment Separation

The central innovation from Ressi. Rather than treating all on-disk files equally (as standard LSM trees do), heliosDB maintains a strict invariant:

> **The active segment always contains the latest version of every live key.**

This changes the scan story fundamentally. A current-snapshot scan iterates a single file (the active segment) sequentially, then consults inactive segments only for keys that are absent from active — gated by negative bloom filters. Compare that to a standard LSM scan, which must merge across all levels simultaneously.

| | Active Segment | Inactive Segments |
|---|---|---|
| Count | Exactly one | Many (leveled) |
| Contents | Latest version of every live key | Historical versions, overflow |
| Bloom filter | N/A | Negative existence filter — skip if key is in active |
| Scan role | Primary target (one sequential read) | Gap-fill only |
| Written by | Flush pipeline (MemTable merge) | Compaction |

### Write Path

```
put(k, v)
  └─ WAL append (crash durability)
  └─ MemTable insert (concurrent skip list)
       │
       │ [MemTable full]
       ▼
  Flush pipeline: merge(MemTable, old active) → new active SST
       │
       └─ old active file deleted
       └─ WAL rotated
```

Every flush **merges** the MemTable with the current active segment, producing a new active that holds the complete latest-version state. This is different from standard LSM, which appends a new L0 file per flush and requires merge-on-read.

### Read Path

```
get(k)
  1. MemTable          — O(log n), in-memory
  2. Active segment    — O(file size / block size), one file, often cached
  3. Inactive segments — O(levels), each gated by a negative bloom filter
```

### File Format

Each SST file (active or inactive) uses a Ressi-inspired block layout:

```
┌──────────────────────────────┐
│  Data Block 0  (compressed)  │  sorted key-value entries, prefix-compressed
│  Data Block 1                │  restart points every 16 entries for binary search
│  ...                         │
│  Data Block N                │
├──────────────────────────────┤
│  Bloom Filter Block          │  double-hashing, ~1% false-positive rate
├──────────────────────────────┤
│  Index Block                 │  last_key → (offset, size) per data block
├──────────────────────────────┤
│  Properties Block            │  entry count, key range, compression type
├──────────────────────────────┤
│  Footer (48 bytes)           │  block handles + magic 0x48454C494F534442
└──────────────────────────────┘
```

Internal keys carry a sequence number and operation type for MVCC-lite ordering:
```
[user_key bytes][seq_num: 7 bytes BE][op_type: 1 byte]
```

Keys within a block are sorted ascending by user_key, then descending by sequence number (latest version first).

## Architecture

heliosDB is structured as a [Cargo workspace](https://doc.rust-lang.org/cargo/reference/workspaces.html) with four crates, each independently benchmarkable:

```
heliosdb/
├── crates/
│   ├── heliosdb-types/     # InternalKey, SeqNum, OpType, HeliosError
│   ├── heliosdb-sst/       # Block, Bloom, Index, SST Builder, SST Reader
│   │   └── benches/        # Layer-isolated benchmarks
│   ├── heliosdb-engine/    # WAL, MemTable, Segments, Compaction, Manifest
│   │   └── benches/
│   └── heliosdb/           # Public DB API + CLI binary
│       └── tests/          # Integration + oracle tests
```

## Usage

Add to `Cargo.toml`:

```toml
[dependencies]
heliosdb = { path = "crates/heliosdb" }
```

```rust
use heliosdb::{DB, Options};

fn main() -> heliosdb::Result<()> {
    let db = DB::open("/tmp/mydb", Options::default())?;

    // Write
    db.put(b"hello", b"world")?;
    db.put(b"foo",   b"bar")?;

    // Read
    assert_eq!(db.get(b"hello")?.as_deref(), Some(b"world".as_ref()));

    // Delete
    db.delete(b"hello")?;
    assert_eq!(db.get(b"hello")?, None);

    // Range scan — returns sorted (key, value) pairs in [start, end)
    let pairs = db.scan(b"a", b"z")?;
    for (k, v) in pairs {
        println!("{} = {}", String::from_utf8_lossy(&k), String::from_utf8_lossy(&v));
    }

    // Force flush MemTable to active segment
    db.flush()?;

    Ok(())
}
```

### Options

```rust
let opts = Options {
    // Flush MemTable when it exceeds this size (default: 64 MiB)
    memtable_size_limit: 64 * 1024 * 1024,

    // Per-block compression codec (default: None)
    compression: heliosdb::CompressionType::Snappy,
    // or: CompressionType::Zstd
    // or: CompressionType::None
};

let db = DB::open("/tmp/mydb", opts)?;
```

### CLI

```bash
# Build
cargo build --release -p heliosdb

# Put / Get / Delete / Scan
cargo run -p heliosdb --bin helios_cli -- /tmp/mydb put hello world
cargo run -p heliosdb --bin helios_cli -- /tmp/mydb get hello
cargo run -p heliosdb --bin helios_cli -- /tmp/mydb delete hello
cargo run -p heliosdb --bin helios_cli -- /tmp/mydb scan a z
cargo run -p heliosdb --bin helios_cli -- /tmp/mydb flush
```

## Benchmarks

Benchmarks are layered so each subsystem can be measured independently:

```bash
# SST layer: block encode/decode, bloom build, SST build throughput
cargo bench -p heliosdb-sst

# Engine layer: MemTable insert TPS, point lookup latency
cargo bench -p heliosdb-engine
```

Key metrics tracked:

| Metric | Description |
|---|---|
| Block encode/decode | Entries/sec and MB/sec at different block sizes |
| Bloom build | Insertion rate for different key counts |
| MemTable insert | Concurrent write throughput |
| MemTable get | Point lookup latency (p50/p95/p99) |

## Testing

```bash
# All tests
cargo test

# Integration tests only (CRUD, WAL recovery, scan oracle, flush)
cargo test -p heliosdb --test integration

# Unit tests per crate
cargo test -p heliosdb-sst
cargo test -p heliosdb-engine
cargo test -p heliosdb-types
```

### Test strategy

| Level | What it covers |
|---|---|
| Unit tests | Roundtrip: block encode/decode, bloom no-false-negatives, WAL replay, MemTable snapshot isolation |
| Integration tests | End-to-end CRUD, WAL crash recovery, flush trigger, tombstone propagation |
| Oracle tests | `scan` result compared against an in-memory `HashMap` for 500 keys with random deletes |
| Active segment invariant | Verified after every flush: latest value always readable |

## Crash Safety

- Every write is appended to the WAL and fsynced before the MemTable is updated.
- On open, the WAL is replayed to recover any MemTable entries not yet flushed to the active segment.
- The active segment is written atomically (write full file, then update manifest).

## Limitations and Roadmap

heliosDB is under active development. Current known limitations:

- **No concurrent writers** — a single `RwLock` serializes all writes. Multi-writer support (lock-free MemTable swap) is planned.
- **Active segment grows with writes** — every flush merges into the active, so the active grows O(total live data). Size-triggered sealing to L1 inactive is the next milestone.
- **No range tombstones** — point deletes only.
- **No column families** — single keyspace.
- **Bloom filter on active segment** — currently the active segment is scanned linearly for point lookups. Adding an in-memory index or bloom filter for the active segment is a near-term improvement.

## Inspiration

- [Google Ressi](https://cloud.google.com/blog/products/databases/spanner-modern-columnar-storage-engine) — columnar LSM storage for Spanner; active/inactive separation, existence filters
- [LevelDB](https://github.com/google/leveldb) — SSTable format, leveled compaction
- [RocksDB](https://rocksdb.org/) — production LSM reference
- [WiscKey](https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf) — key-value separation for large values
- [FoundationDB](https://apple.github.io/foundationdb/testing.html) — deterministic simulation testing approach

## License

MIT
