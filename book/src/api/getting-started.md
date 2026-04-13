# Getting Started

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
