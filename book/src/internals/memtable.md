# MemTable

**Crate**: `heliosdb-engine`  **Files**: `src/memtable/`

The memtable layer is split into three concerns, each in its own file:

| File | Responsibility |
|---|---|
| `traits.rs` | `MemTable` trait — the interface every implementation must satisfy |
| `skiplist.rs` | `SkipListMemTable` — lock-free concurrent skip list (default) |
| `btree.rs` | `BTreeMemTable` — `RwLock<BTreeMap>` alternative |
| `memtable_set.rs` | `MemTableSet` — one mutable + N immutable memtables |

---

## The `MemTable` trait

Every implementation must satisfy:

```rust
pub trait MemTable: Default + Send + Sync {
    fn put(&self, user_key: UserKey, seq_num: SeqNum, value: Value);
    fn delete(&self, user_key: UserKey, seq_num: SeqNum);
    fn get(&self, user_key: &[u8], read_seq: SeqNum) -> Option<GetResult>;
    fn size_bytes(&self) -> usize;
    fn is_empty(&self) -> bool;
    fn iter(&self) -> Vec<(InternalKey, Value)>;
}
```

`Default + Send + Sync` are required because `DB` holds a memtable behind a
`RwLock` and must be able to create a fresh empty one after a flush.

`DB` is generic over `M: MemTable`:

```rust
pub struct DB<M = SkipListMemTable> { ... }

let db = DB::<SkipListMemTable>::open("/tmp/db", opts)?;
```

---

## Snapshot reads

Every read provides a `read_seq` — the sequence number at the time the read
started. The `get` implementation returns the latest version of a key whose
`seq_num ≤ read_seq`.

`InternalKey` ordering is ascending by `user_key`, **descending** by `seq_num`
(latest version sorts first). Both implementations exploit this with the same
probe trick:

```rust
let probe = InternalKey::new_put(Bytes::copy_from_slice(user_key), read_seq);
```

- **SkipListMemTable**: `map.lower_bound(Included(&probe))`
- **BTreeMemTable**: `map.range(probe..).next()`

This implements **MVCC-lite**: writes at `seq_num > read_seq` are invisible,
enabling consistent point-in-time reads even under concurrent writes.

---

## `SkipListMemTable`

Backed by `crossbeam_skiplist::SkipMap`. Readers and writers never block each
other — the skip list uses epoch-based lock-free concurrency.

**Strengths**: fast negative lookups; readers and writers run concurrently.

**Weakness**: epoch-based memory reclamation adds overhead to iteration
(the flush path).

---

## `BTreeMemTable`

Backed by `parking_lot::RwLock<BTreeMap<InternalKey, Value>>`.

**Strengths**: iteration is ~2.4× faster than the skip list (contiguous memory,
no pointer-chasing) — important for the flush pipeline.

**Weakness**: reads and writes are mutually exclusive (write lock excludes all
readers); negative lookups are ~2× slower than the skip list.

### Benchmark comparison (100k entries)

| Operation | SkipList | BTree | Winner |
|---|---|---|---|
| Insert | 86 ms | 96 ms | SkipList ~11% |
| Get hit | 549 ns | 521 ns | ~tie |
| Get miss | 280 ns | 550 ns | SkipList 2× |
| Delete | 74 ms | 72 ms | BTree ~3% |
| Iter (flush) | 18.5 ms | 10.7 ms | BTree 2.4× |

Choose **SkipList** for read-heavy or mixed workloads.
Choose **BTree** if flush throughput is the bottleneck.

---

## `MemTableSet` — active + immutable queue

`MemTableSet<M>` manages the full lifecycle of memtables inside `DB`.

### Structure

```
writes ──► [ active: M ]
                 │  size >= write_buffer_size → rotate_arc()
                 ▼
          [ immutable[n] ]  ← newest (Arc<M>)
          [ immutable[1] ]
          [ immutable[0] ]  ← oldest, flushed first
                 │  bounded channel → flusher thread
                 ▼
           L0 SST on disk
```

```rust
pub struct MemTableSet<M> {
    active:           M,
    immutable:        VecDeque<Arc<M>>,  // front = oldest, back = newest
    write_buffer_size: usize,
    max_immutable:    usize,
}
```

Immutable memtables are stored as `Arc<M>` so they can be shared between
the read path (VecDeque) and the background flusher (channel) without
copying.

### Rotation

When `active.size_bytes() >= write_buffer_size`, the write path calls
`rotate_arc()`:

1. Seal the active memtable → `Arc<M>`.
2. Push the Arc to the immutable VecDeque (stays readable).
3. Return the Arc to the caller (for sending to the flusher channel).
4. Replace `active` with a fresh `M::default()`.

The write lock is released **before** the channel send. If the channel is
full (capacity = `max_immutable_count`), the writer blocks — this is the
**backpressure point**, and no lock is held during the wait.

### Background flusher

A dedicated `helios-flusher` thread receives sealed `Arc<M>` from the
bounded channel and writes each one to a new level-0 SST file:

```
recv(Arc<M>) → write lock → pop oldest immutable → write SST → register
```

Each flush creates a standalone SST — no merging with existing files.

### Read path

`MemTableSet::get` checks sources in priority order — most recent first:

```
active → immutable[newest] → ... → immutable[oldest]
```

Returns on the first match. A tombstone in the active memtable hides a value
in any immutable below it.

### WAL safety

Rotating the active memtable into the immutable queue does **not** rotate the
WAL. The WAL continues to cover all writes across every immutable and the
active memtable. The WAL is only truncated during an explicit `db.flush()`,
after every immutable has been flushed to SST.

On crash recovery, all WAL entries are replayed into a single fresh active
memtable. The immutable queue always starts empty.

### `Options`

```rust
pub struct Options {
    /// Seal the active memtable at this size. Default: 64 MiB.
    pub write_buffer_size: usize,

    /// Bounded channel capacity / max immutable memtables.
    /// Writers block when this many sealed memtables are pending. Default: 2.
    pub max_immutable_count: usize,

    pub compression: CompressionType,
}
```

With `max_immutable_count = 2`, heliosDB can absorb two full write buffers
before stalling — giving the flusher thread time to catch up.
