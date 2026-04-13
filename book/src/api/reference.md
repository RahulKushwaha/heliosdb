# API Reference

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
