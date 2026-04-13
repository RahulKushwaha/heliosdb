# Active / Inactive Separation

## Background

heliosDB's flush model is inspired by Google Ressi's single-authoritative-file
idea but currently uses a simpler approach: each memtable flush writes a
separate level-0 SST file. Compaction later merges overlapping L0 files
into non-overlapping higher levels.

## Flush pipeline

```
MemTable full
  │
  ├─ rotate_arc()
  │    Seal active memtable → Arc<M>
  │    Push to immutable VecDeque (reads) + bounded channel (flusher)
  │
  └─ flusher thread
       Pop from channel → write entries to new L0 SST
       Register in manifest (AddInactive level=0)
```

Each flush creates a standalone SST — no merging with existing files.
This keeps the flush path simple and predictable.

## Level-0 overlap

Unlike higher levels, level-0 SSTs can have overlapping key ranges
because each one is an independent memtable snapshot:

```
L0 SST #3 (newest):  [apple..cherry]   ← delete banana
L0 SST #2:           [banana..fig]     ← put banana=yellow
L0 SST #1 (oldest):  [apple..date]     ← put apple=fruit
```

The read path handles this:
- **Point lookups** iterate L0 SSTs newest-first, stopping at the first
  match (including tombstones).
- **Scans** iterate L0 SSTs oldest-first; each layer overwrites the
  previous, so the newest version wins.

## Bloom filter as a negative existence filter

Inactive segment bloom filters are indexed by the full encoded InternalKey.
For level-0 SSTs created by the flush pipeline, point lookups scan entries
directly rather than relying on the bloom filter (since the filter is keyed
on InternalKey encoding, not raw user keys).

Higher-level segments (produced by compaction) use bloom filters effectively
to skip segments that definitely don't contain a given key.

## Compaction

Compaction reduces L0 overlap by merging multiple L0 SSTs into larger,
non-overlapping higher-level files. This improves read performance over
time by reducing the number of files that must be checked.
