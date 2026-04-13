# Write & Read Paths

## Write path

```
db.put(key, value)
  │
  ├─ 1. Allocate sequence number (atomic fetch-add)
  │
  ├─ 2. Acquire write lock
  │      ├─ Append to WAL (CRC-framed record)
  │      ├─ Insert into MemTable (concurrent skip list)
  │      │      InternalKey = user_key + seq_num + Put
  │      └─ [if MemTable full] rotate_arc()
  │              Seal the active memtable → Arc<M>
  │              Push Arc to immutable VecDeque (for reads)
  │
  ├─ 3. Release write lock
  │
  └─ 4. [if rotated] send Arc<M> to bounded channel
              Blocks if channel full (backpressure).
              Background flusher receives it and writes to SST.
```

`db.delete(key)` follows the same path but writes a **tombstone** entry
(`op_type = Delete`) rather than a value.

### Background flusher

A dedicated `helios-flusher` thread receives sealed memtables from the
bounded channel and writes each one to a new level-0 SST file:

```
flusher thread
  │
  loop {
    recv(sealed_memtable)   ← blocks until work arrives
    │
    ├─ Acquire write lock
    │    ├─ Pop oldest immutable from VecDeque
    │    ├─ Write sealed memtable entries to new L0 SST
    │    ├─ Register SST as inactive segment (level 0)
    │    └─ Append AddInactive + SetNextSeq to Manifest
    └─ Release write lock
  }
```

The channel capacity equals `max_immutable_count` (default 2). When the
channel is full, the writer's `send()` blocks — this is the **backpressure
point**. No lock is held during the wait.

### Explicit flush

`db.flush()` bypasses the background flusher and runs synchronously:

1. Rotate the active memtable (if non-empty).
2. Drain all immutables to SST files (oldest first).
3. Truncate the WAL (all data is now on disk).

## Read path (point lookup)

```
db.get(key)
  │
  ├─ 1. MemTableSet.get(key, read_seq)
  │      Checks active → newest immutable → oldest immutable.
  │      Returns Value, Tombstone, or None.
  │
  ├─ 2. [if None] ActiveSegment (legacy, from recovery)
  │      Scan entries in sorted order.
  │
  └─ 3. [if None] InactiveSegments (newest-first)
              For each segment, scan for matching user_key.
              A tombstone stops the search (key was deleted).
```

In the common case (recently written key), step 1 returns immediately.
For flushed keys, step 3 finds them in the newest level-0 SST.

## Scan path

```
db.scan(start, end)
  │
  ├─ 1. Collect from inactive segments (oldest → newest)
  │      Each layer overwrites the previous (insert for puts,
  │      remove for deletes). Within each SST, only the first
  │      visible version of each key is used (highest seq ≤ read_seq).
  │
  ├─ 2. Overwrite with active segment entries (legacy)
  │
  └─ 3. Overwrite with MemTableSet entries (highest priority)
              oldest immutable → newest immutable → active

Result: sorted (key, value) pairs, latest version per key.
```

## Crash recovery

On `DB::open`:
1. Read `MANIFEST` → reconstruct `VersionSet` (which SST files exist, at which levels).
2. Replay `WAL` → reconstruct the MemTable for any writes not yet flushed.
3. Open the active segment and inactive segments listed in the VersionSet.

If the process died mid-flush, a partially written SST may exist on disk but
won't be recorded in the Manifest. Recovery always opens a consistent state.
