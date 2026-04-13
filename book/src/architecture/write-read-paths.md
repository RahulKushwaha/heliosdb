# Write & Read Paths

## Write path

```
db.put(key, value)
  │
  ├─ 1. Allocate sequence number (atomic fetch-add)
  │
  ├─ 2. Append to WAL (CRC-framed record, fsync)
  │      Guarantees durability before the MemTable is updated.
  │
  ├─ 3. Insert into MemTable (concurrent skip list)
  │      InternalKey = user_key + seq_num + Put
  │
  └─ 4. [if MemTable size > limit] trigger flush
              │
              ├─ Read all entries from current active segment
              ├─ Merge with MemTable (MemTable wins on same user_key)
              ├─ Write merged output to new active SST file
              ├─ Delete old active SST file
              ├─ Append SetActive + SetNextSeq to Manifest
              └─ Replace MemTable with empty, rotate WAL
```

`db.delete(key)` follows the same path but writes a **tombstone** entry
(`op_type = Delete`) rather than a value.

## Read path (point lookup)

```
db.get(key)
  │
  ├─ 1. MemTable.get(key, read_seq)
  │      O(log n), in memory. Returns Value or Tombstone or None.
  │
  ├─ 2. [if None] ActiveSegment scan
  │      Scan entries in sorted order; stop when user_key > search_key.
  │      Returns the first matching entry.
  │
  └─ 3. [if None] InactiveSegments (newest-first)
              For each:
                if definitely_not_here(key) → skip (bloom filter)
                else scan segment for matching user_key
```

In the common case (recently written key), step 1 returns immediately.
For keys flushed to the active segment, step 2 hits it.
Inactive segments are consulted only for historical/overflow data.

## Scan path

```
db.scan(start, end)
  │
  ├─ 1. Collect from inactive segments (lowest priority)
  │      Iterate all entries, filter by key range and read_seq.
  │
  ├─ 2. Overwrite with active segment entries (higher priority)
  │      Active segment is the authoritative source for live keys.
  │      Tombstones remove keys seen from inactive.
  │
  └─ 3. Overwrite with MemTable entries (highest priority)
              MemTable has the most recent writes.

Result: sorted (key, value) pairs, latest version per key.
```

The active segment read in step 2 is a **single sequential file scan** — this is
where the active/inactive separation pays off for scan performance. In the common
case (all keys are live), step 1 produces nothing and the scan is effectively
O(active_segment_size).

## Crash recovery

On `DB::open`:
1. Read `MANIFEST` → reconstruct `VersionSet` (which SST files exist, at which levels).
2. Replay `WAL` → reconstruct the MemTable for any writes not yet flushed.
3. Open the active segment and inactive segments listed in the VersionSet.

If the process died mid-flush, the new active SST may be partially written or
absent. The Manifest only records the new active path after the file is fully
written, so recovery always opens a consistent state.
