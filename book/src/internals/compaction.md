# Compaction

**Crate**: `heliosdb-engine`  **File**: `src/compaction/mod.rs`

Compaction merges inactive segments to bound space amplification and clean up
tombstones. It **never touches the active segment** — that file is exclusively
managed by the flush pipeline.

## Level structure

heliosDB uses **leveled compaction** for inactive segments:

| Level | Size limit | Key ranges |
|---|---|---|
| L1 | 10 MB | May overlap (recently sealed actives) |
| L2 | 100 MB | Non-overlapping |
| L3 | 1 GB | Non-overlapping |
| L4+ | × 10 per level | Non-overlapping |

L1 is where newly sealed active segments land. When L1 exceeds 10 MB, its files
are merged and pushed to L2. The process cascades down the levels as needed.

## Compaction trigger

`Compactor::maybe_compact()` is called after every flush. It scans levels 1–6
and finds the first whose total file size exceeds the limit:

```rust
for level in 1u32..=6 {
    let total_size: u64 = version.inactive_at_level(level)
        .iter()
        .filter_map(|p| fs::metadata(p).ok())
        .map(|m| m.len())
        .sum();

    if total_size > level_limit(level) {
        self.compact_level(level)?;
        return Ok(true);
    }
}
```

## Compaction process

For a level that needs compaction:

1. Open all inactive segments at that level and collect their entries.
2. Feed them through `MergeIterator` (heap-based N-way merge).
3. Write the merged output to a new SST at `level + 1`.
4. Update the Manifest: add the new file, remove the old ones.
5. Delete the old SST files from disk.

```
L1: [file_a.sst] [file_b.sst] [file_c.sst]
       ↓ merge (MergeIterator)
L2: [compacted_001.sst]
```

## Tombstone removal

During compaction, tombstones (`op_type = Delete`) are removed when compacting
to the deepest supported level (L6). At that point, no lower level can hold an
older version of the key, so the tombstone has served its purpose.

At shallower levels, tombstones are preserved so that reads at higher levels
can see the deletion.

## Write amplification

Each key written by the user may be rewritten during compaction. The write
amplification factor (WAF) for leveled compaction is approximately:

```
WAF ≈ 1 (initial write) + L (one rewrite per level during compaction)
    = 1 + num_levels
```

With 6 levels, WAF ≈ 7 in the worst case. In practice, not all levels trigger
simultaneously, so average WAF is lower.

## Manifest atomicity

The Manifest is an append-only log. Compaction records:
1. `AddInactive` for the new output file.
2. `RemoveInactive` for each input file.

If the process crashes during compaction, the next open replays the Manifest.
If only `AddInactive` was written (crash before `RemoveInactive`), both old and
new files exist — the old files are kept and the new one is an orphan. A future
`maybe_compact()` call will clean this up.
