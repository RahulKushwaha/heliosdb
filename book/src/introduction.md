# Introduction

heliosDB is a production-grade embedded key-value storage engine written in Rust,
inspired by [Google Ressi](https://cloud.google.com/blog/products/databases/spanner-modern-columnar-storage-engine)
— the columnar LSM-based file format that powers Google Spanner.

## What heliosDB is

- An **embedded** KV store: link it into your process, no network roundtrips.
- **LSM-based**: optimized for write-heavy workloads with sequential I/O.
- **Ressi-inspired**: borrows Ressi's active/inactive segment separation to make
  scan performance competitive with B-trees, without sacrificing write throughput.

## What makes it different

Standard LSM trees (LevelDB, RocksDB) keep recent writes in a pile of L0 files.
A scan must merge across all of them simultaneously — more files means more CPU.

heliosDB maintains a single **active segment** that always holds the latest version
of every live key. A scan reads one file sequentially, then fills gaps from
immutable **inactive segments** gated by negative bloom filters.

## Quick start

```rust
use heliosdb::{DB, Options};

let db = DB::open("/tmp/mydb", Options::default())?;

db.put(b"hello", b"world")?;
assert_eq!(db.get(b"hello")?.as_deref(), Some(b"world".as_ref()));
db.delete(b"hello")?;

let pairs = db.scan(b"a", b"z")?;
```

## Workspace layout

heliosDB is split into four crates so each layer can be benchmarked independently:

| Crate | Role |
|---|---|
| `heliosdb-types` | `InternalKey`, `SeqNum`, `OpType`, `HeliosError` |
| `heliosdb-sst` | Block format, Bloom filter, Index block, SST Builder/Reader |
| `heliosdb-engine` | WAL, MemTable, Segments, Flush pipeline, Compaction, Manifest |
| `heliosdb` | Public `DB` API + `helios_cli` binary |
