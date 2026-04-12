pub fn arch_overview() -> &'static str {
    r#"# Architecture Overview

heliosDB is organized as a four-crate Cargo workspace. Each crate has a strict
dependency direction — lower crates never depend on higher ones — which keeps the
layers independently testable and benchmarkable.

```
heliosdb          (public API, CLI)
    └── heliosdb-engine   (WAL, MemTable, Segments, Compaction, Manifest)
            └── heliosdb-sst      (Block, Bloom, Index, SST Builder/Reader)
                    └── heliosdb-types    (InternalKey, Error, SeqNum)
```

## Component map

```
┌────────────────────────────────────────────────┐
│                 heliosdb (DB)                  │
│  put / get / delete / scan / flush             │
│                                                │
│  ┌──────────────┐   ┌──────────────────────┐  │
│  │  MemTable    │   │  ActiveSegment        │  │
│  │  (skip list) │   │  (one SST, all live   │  │
│  │              │   │   keys, latest ver.)  │  │
│  └──────┬───────┘   └──────────┬───────────┘  │
│         │  flush                │ seal          │
│         └──────────────────────▼              │
│                         InactiveSegments       │
│                         (historical, bloom-    │
│                          filtered reads)       │
│                                                │
│  ┌──────────────┐   ┌──────────────────────┐  │
│  │  WAL         │   │  Manifest            │  │
│  │  (crash      │   │  (version edits,     │  │
│  │   recovery)  │   │   file membership)   │  │
│  └──────────────┘   └──────────────────────┘  │
└────────────────────────────────────────────────┘
```

## Key invariant

> The active segment always contains the latest version of every currently-live key.

This invariant is established by the flush pipeline and never broken:
- Every `put` lands in the MemTable.
- Every flush merges the MemTable **into** the active segment (not alongside it).
- The result is a new active segment with up-to-date values for all keys.

Inactive segments hold historical data and overflow from older compaction cycles.
They are never needed for current-snapshot reads unless a key is absent from the
active segment — which the negative bloom filter catches cheaply.
"#
}

pub fn arch_active_inactive() -> &'static str {
    r#"# Active / Inactive Separation

This is the central design idea inherited from Google Ressi.

## The problem with standard LSM scans

In a standard LSM tree (LevelDB, RocksDB), a scan must merge-iterate across
every level simultaneously — MemTable, L0 files (which can overlap), L1, L2, ...
The merge fan-in is `O(L0_files + num_levels)`. With 4 L0 files and 6 levels,
that's 10 concurrent merge streams for every single scan.

More critically, the merge iterator must compare sequence numbers on every key to
figure out which version wins. This is pure CPU overhead for the common case
where you just want the latest value.

## The Ressi solution: one authoritative file

heliosDB maintains exactly one **active segment** that is the sole authoritative
source for current values:

```
Active Segment
┌─────────────────────────────────────┐
│  "apple"  → "fruit"    (seq=1042)  │
│  "banana" → "yellow"   (seq=997)   │
│  "cherry" → "red"      (seq=1051)  │
│  ...                               │
│  (sorted, latest version per key)  │
└─────────────────────────────────────┘

Inactive Segments (historical only)
┌──────────────┐  ┌──────────────┐
│  L1 file     │  │  L2 file     │
│  (older ver) │  │  (older ver) │
└──────────────┘  └──────────────┘
```

A scan for current values:
1. Iterate the active segment sequentially — **one file, one pass**.
2. For keys absent from the active, consult inactive segments.
   Each inactive segment's bloom filter answers "is this key definitely NOT here?"
   Most lookups are skipped entirely.

## Scan complexity comparison

| Approach | Scan streams |
|---|---|
| Standard LSM (L0=4, 6 levels) | ≤10 merge streams |
| heliosDB (active/inactive) | 1 active + targeted gap-fills |

For a full-keyspace scan where every key is live, the inactive segments are
never consulted at all — it's a single sequential read of the active segment.

## The flush invariant

The invariant is maintained by the flush pipeline:

```
Old state:
  active = { a→v1, b→v2 }
  MemTable = { b→v3, c→v4 }   ← b was updated

Flush merge:
  MergeIterator(MemTable priority=0, active priority=1)
  → a→v1  (only in active)
  → b→v3  (MemTable wins, higher seq_num)
  → c→v4  (only in MemTable)

New state:
  active = { a→v1, b→v3, c→v4 }   ← invariant restored
  old active file deleted
```

## Bloom filter as a negative existence filter

Inactive segment bloom filters are indexed by user key. The query
`definitely_not_here(key)` returns `true` when the key is provably absent from
that inactive file. This allows the read path to skip most inactive segments
without any disk I/O.

False negatives are impossible by Bloom filter construction — if a key is in the
file, the filter always says "might be here". Only false positives (unnecessary
reads) can occur, at a tunable ~1% rate.
"#
}

pub fn arch_file_format() -> &'static str {
    r#"# File Format

Every SST file in heliosDB — active or inactive — uses the same on-disk layout,
inspired by Ressi's block-based design.

## Overall layout

```
┌──────────────────────────────────────────────┐
│  Data Block 0                                │
│  Data Block 1                                │
│  ...                                         │
│  Data Block N                                │
├──────────────────────────────────────────────┤
│  Bloom Filter Block                          │
├──────────────────────────────────────────────┤
│  Index Block                                 │
├──────────────────────────────────────────────┤
│  Properties Block                            │
├──────────────────────────────────────────────┤
│  Footer (48 bytes)                           │
└──────────────────────────────────────────────┘
```

All integers are little-endian.

## Data blocks

Target size: **64 KiB** (before compression). Entries within a block are sorted
by InternalKey and use **prefix compression** with restart points:

```
Entry:
  [shared_key_len: u16]    bytes shared with the previous key
  [unshared_key_len: u16]  bytes unique to this key
  [value_len: u32]
  [key_delta: bytes]       only the non-shared suffix
  [value: bytes]

Every 16 entries: restart point (shared_key_len = 0, full key stored)

Block trailer:
  [restart_offsets: u32 * N]  byte offsets of each restart point
  [num_restarts: u32]
  [crc32: u32]                checksum of compressed content
  [compression_type: u8]      0=None, 1=Snappy, 2=Zstd
```

Restart points enable **binary search** within a block: jump to a restart point,
then scan forward.

## Bloom filter block

Double-hashing bloom filter tuned for ~1% false-positive rate:

```
[bit_array: ceil(num_bits/8) bytes]
[num_hash_fns: u8]
```

`num_bits` is always rounded up to a byte boundary so that the encoded
`bit_bytes * 8 == num_bits` exactly on decode (avoids false negatives from
bit-position mismatches).

## Index block

Maps the **last key of each data block** to its `(offset, size)` handle:

```
For each data block:
  [key_len: u16][last_key: bytes][offset: u64][size: u32]
```

A lookup does binary search: find the first entry whose `last_key >= search_key`,
then load that data block.

## Footer (48 bytes)

```
[bloom_handle:  offset u64 + size u32 = 12 bytes]
[index_handle:  12 bytes]
[props_handle:  12 bytes]
[padding:       4 bytes]
[magic:         u64]  = 0x48454C494F534442  ("HELIOSDB")
```

The magic number is checked on open; a wrong magic means a corrupted or
incompatible file.

## InternalKey encoding

Keys stored in SST blocks are **encoded InternalKeys**, not raw user keys:

```
[user_key bytes][seq_num: 7 bytes big-endian][op_type: 1 byte]
```

Ordering within a block:
- **Ascending** by user_key
- **Descending** by seq_num (latest version of a key sorts first)

`op_type` is `0` for `Put` and `1` for `Delete` (tombstone).
"#
}

pub fn arch_write_read_paths() -> &'static str {
    r#"# Write & Read Paths

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
"#
}
