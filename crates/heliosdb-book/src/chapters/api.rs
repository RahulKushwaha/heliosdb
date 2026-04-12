pub fn api_getting_started() -> &'static str {
    r#"# Getting Started

## Installation

Add heliosDB to your `Cargo.toml`:

```toml
[dependencies]
heliosdb = { path = "../heliosdb/crates/heliosdb" }
# or once published:
# heliosdb = "0.1"
```

## Opening a database

```rust
use heliosdb::{DB, Options};

fn main() -> heliosdb::Result<()> {
    // Opens an existing DB or creates a new one.
    // The directory is created if it doesn't exist.
    let db = DB::open("/tmp/mydb", Options::default())?;

    Ok(())
}
```

## Basic operations

```rust
// Write a key-value pair
db.put(b"hello", b"world")?;

// Read it back
match db.get(b"hello")? {
    Some(value) => println!("{}", String::from_utf8_lossy(&value)),
    None        => println!("not found"),
}

// Overwrite
db.put(b"hello", b"updated")?;

// Delete
db.delete(b"hello")?;
assert_eq!(db.get(b"hello")?, None);
```

## Range scan

`scan` returns all live key-value pairs in the range `[start, end)`, sorted by key:

```rust
// Write some data
for i in 0u32..10 {
    db.put(format!("user:{i:03}").as_bytes(), format!("data{i}").as_bytes())?;
}

// Scan all "user:" keys
let pairs = db.scan(b"user:", b"user:~")?;
for (key, value) in &pairs {
    println!("{} = {}", String::from_utf8_lossy(key), String::from_utf8_lossy(value));
}
// user:000 = data0
// user:001 = data1
// ...
```

## Forcing a flush

heliosDB automatically flushes the MemTable when it reaches the configured size
limit. You can also trigger a flush manually — useful before a clean shutdown:

```rust
db.flush()?;
```

## Crash recovery

heliosDB recovers automatically on `DB::open`. The WAL is replayed to restore
any writes that were acknowledged but not yet flushed to the active segment:

```rust
{
    let db = DB::open("/tmp/mydb", Options::default())?;
    db.put(b"durable", b"yes")?;
    // process exits here without explicit flush
}

// On next open, WAL replay restores "durable"
let db = DB::open("/tmp/mydb", Options::default())?;
assert_eq!(db.get(b"durable")?.as_deref(), Some(b"yes".as_ref()));
```

## CLI

A command-line tool is included for manual inspection and testing:

```bash
cargo build --release -p heliosdb

export DB=/tmp/mydb

cargo run -p heliosdb --bin helios_cli -- $DB put hello world
cargo run -p heliosdb --bin helios_cli -- $DB get hello
# world

cargo run -p heliosdb --bin helios_cli -- $DB put foo bar
cargo run -p heliosdb --bin helios_cli -- $DB scan a z
# foo = bar
# hello = world

cargo run -p heliosdb --bin helios_cli -- $DB delete hello
cargo run -p heliosdb --bin helios_cli -- $DB flush
```
"#
}

pub fn api_reference() -> &'static str {
    r#"# API Reference

## `DB`

The top-level database handle. Cheap to clone (internally reference-counted).

### `DB::open`

```rust
pub fn open(dir: impl AsRef<Path>, opts: Options) -> Result<Self>
```

Opens or creates a database at `dir`. Creates the directory if it doesn't exist.
Replays the WAL to recover any unflushed writes from the previous session.

**Errors**: `HeliosError::Io` if the directory cannot be created or files cannot
be opened.

---

### `DB::put`

```rust
pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()>
```

Write a key-value pair. The write is durable once `put` returns — it has been
appended to the WAL and fsynced.

Overwrites any existing value for `key`.

**Thread safety**: safe to call concurrently from multiple threads.

---

### `DB::get`

```rust
pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>>
```

Look up a key. Returns `None` if the key does not exist or has been deleted.

Read order: MemTable → ActiveSegment → InactiveSegments (bloom-filtered).

---

### `DB::delete`

```rust
pub fn delete(&self, key: &[u8]) -> Result<()>
```

Delete a key. Writes a tombstone entry; the key becomes invisible to subsequent
reads. The tombstone is physically removed during compaction.

---

### `DB::scan`

```rust
pub fn scan(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Bytes, Bytes)>>
```

Return all live key-value pairs in the half-open range `[start, end)`, sorted
by key in ascending order.

The result is a snapshot: concurrent writes after `scan` begins are not visible.

**Performance**: the active segment is read sequentially as the primary source.
Inactive segments are consulted only for keys absent from the active segment.

---

### `DB::flush`

```rust
pub fn flush(&self) -> Result<()>
```

Force a flush of the MemTable to the active segment, even if the size limit has
not been reached. Useful before a planned shutdown to minimize recovery time.

---

## `Options`

```rust
pub struct Options {
    /// Flush MemTable to disk when it exceeds this many bytes.
    /// Default: 64 MiB
    pub memtable_size_limit: usize,

    /// Per-block compression codec.
    /// Default: CompressionType::None
    pub compression: CompressionType,
}
```

### `CompressionType`

```rust
pub enum CompressionType {
    None,    // No compression. Best for already-compressed payloads.
    Snappy,  // Fast, moderate ratio (~2×). Good default for most workloads.
    Zstd,    // Higher ratio (~3-4×), slower. Good for cold/archival data.
}
```

---

## `HeliosError`

```rust
pub enum HeliosError {
    Io(std::io::Error),         // File system error
    Corruption(String),         // Checksum mismatch, bad magic, truncated data
    NotFound,                   // Key does not exist
    InvalidArgument(String),    // Bad parameter
    Compression(String),        // Codec error
    Closed,                     // DB was closed
}
```

---

## `Result<T>`

```rust
pub type Result<T> = std::result::Result<T, HeliosError>;
```

All public APIs return `heliosdb::Result<T>`.
"#
}
