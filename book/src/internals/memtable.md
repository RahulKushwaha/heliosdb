# MemTable

**Crate**: `heliosdb-engine`  **File**: `src/memtable/mod.rs`

The MemTable is the in-memory write buffer. All writes land here first, before
being flushed to the active segment.

## Data structure

heliosDB uses a **concurrent skip list** from the `crossbeam-skiplist` crate.

A skip list is a probabilistic sorted data structure with O(log n) insert and
lookup, similar to a balanced BST but with simpler concurrency semantics:
readers and writers do not block each other.

The skip list key is `InternalKey` and the value is raw bytes. The ordering is:
1. Ascending by `user_key`
2. Descending by `seq_num` (latest version sorts first within a key)

## Snapshot reads

Every read provides a `read_seq` — the sequence number at the time the read
started. The MemTable returns the latest version of a key whose `seq_num ≤ read_seq`.

```rust
// Probe key: user_key with max possible seq_num for this snapshot
let probe = InternalKey::new_put(user_key.into(), read_seq);

// lower_bound gives the first key >= probe.
// Because seq_num is descending, this is the latest version ≤ read_seq.
let entry = map.lower_bound(Bound::Included(&probe))?;
```

This implements **MVCC-lite**: writes at seq_num > read_seq are invisible to
the reader, enabling consistent point-in-time reads even under concurrent writes.

## Size tracking

`MemTable` tracks approximate memory usage with an atomic counter incremented on
every insert. When `size_bytes()` exceeds `Options::memtable_size_limit`, the DB
triggers an automatic flush.

The size estimate counts `user_key.len() + 8 (trailer) + value.len()` per entry.
It does not account for skip list node overhead, so it slightly underestimates.

## Iteration for flush

`MemTable::iter()` snapshots all entries into a `Vec<(InternalKey, Value)>` and
returns a `MemTableIter`. This is the input to the flush merge pipeline.

The snapshot approach is safe because the MemTable is replaced with a new empty
one after the flush completes — the old one is dropped.

## API

```rust
let mt = MemTable::new();

// Write
mt.put(Bytes::from("hello"), seq_num, Bytes::from("world"));
mt.delete(Bytes::from("hello"), seq_num + 1);

// Read (snapshot at seq_num)
match mt.get(b"hello", read_seq) {
    Some(GetResult::Value(v))  => println!("{v:?}"),
    Some(GetResult::Tombstone) => println!("deleted"),
    None                       => println!("not found"),
}

// Size pressure
if mt.size_bytes() > limit { flush(); }
```
