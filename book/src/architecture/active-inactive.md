# Active / Inactive Separation

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
