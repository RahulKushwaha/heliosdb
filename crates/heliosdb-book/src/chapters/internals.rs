pub fn internals_block() -> &'static str {
    r#"# Block Encoder / Decoder

**Crate**: `heliosdb-sst`  **File**: `src/block.rs`

Data blocks are the fundamental unit of storage in heliosDB. All key-value pairs
live inside data blocks; everything else (bloom filter, index, footer) exists to
help you find the right block quickly.

## Prefix compression

Consecutive keys in a block often share a common prefix (e.g., `"user:001"`,
`"user:002"`, ...). heliosDB exploits this with **prefix compression**:

```
Entry format:
  shared_key_len  : u16   bytes shared with the previous key
  unshared_key_len: u16   bytes unique to this key
  value_len       : u32
  key_delta       : bytes only the non-shared suffix
  value           : bytes
```

For a sequence of keys `apple`, `apricot`, `banana`:
```
apple   → shared=0, unshared=5, delta="apple"
apricot → shared=2, unshared=5, delta="ricot"   (shares "ap")
banana  → shared=0, unshared=6, delta="banana"
```

This significantly reduces on-disk size for clustered key ranges.

## Restart points

Prefix compression requires reading entries in order from the start of the block.
To enable **binary search** within a block, heliosDB inserts a **restart point**
every 16 entries: a restart point stores the full key (shared_key_len = 0).

The block trailer stores the byte offset of each restart point, enabling binary
search over restart points and then a short forward scan to the target key.

```
block layout:
  [entry 0]  ← restart point 0 (full key)
  [entry 1]
  ...
  [entry 15]
  [entry 16] ← restart point 1 (full key)
  ...
  [restart_offset_0: u32]
  [restart_offset_1: u32]
  ...
  [num_restarts: u32]
  [crc32: u32]
  [compression_type: u8]
```

## Compression

Each block is independently compressed before the CRC is computed:

```
raw_entries → compress(codec) → compressed_bytes
checksum = crc32(compressed_bytes)
on-disk = compressed_bytes + checksum + compression_type_byte
```

Supported codecs (configured per-database via `Options`):

| Codec | Trade-off |
|---|---|
| `None` | Zero overhead, best for already-compressed data |
| `Snappy` | Fast compression/decompression, moderate ratio (~2×) |
| `Zstd` | Higher ratio (~3-4×), slower, good for cold data |

The compression type byte is stored in each block independently, allowing future
migration between codecs without rewriting all blocks.

## Block size target

`BlockBuilder` flushes a data block to the SST file when the uncompressed size
estimate exceeds **64 KiB**. This is tunable at the source level. Larger blocks
improve sequential scan throughput (fewer index lookups) at the cost of read
amplification for point lookups.

## API

```rust
// Building a block
let mut builder = BlockBuilder::new(CompressionType::Snappy);
builder.add(b"apple",   b"fruit");
builder.add(b"apricot", b"stone fruit");
builder.add(b"banana",  b"yellow");
let encoded: Bytes = builder.finish()?;

// Decoding a block
let decoder = BlockDecoder::decode(encoded)?;

// Sequential scan
for (key, value) in decoder.iter() { ... }

// Binary search (seek to first key >= target)
if let Some((key, value)) = decoder.seek(b"apricot") { ... }
```
"#
}

pub fn internals_bloom() -> &'static str {
    r#"# Bloom Filter

**Crate**: `heliosdb-sst`  **File**: `src/bloom.rs`

heliosDB uses a **double-hashing bloom filter** to avoid unnecessary disk reads.

## Role in the read path

| Segment type | Filter use |
|---|---|
| Active segment | No bloom filter — it's always consulted directly |
| Inactive segment | `definitely_not_here(key)` — returns `true` if key is **provably absent** |

When `definitely_not_here` returns `true`, the inactive segment is skipped
entirely — no disk I/O, no block decode.

## Construction

For a target false-positive rate *p* and expected key count *n*, the optimal
parameters are:

```
num_bits     = ceil(n × (−ln p) / (ln 2)²)   rounded up to byte boundary
num_hash_fns = round((num_bits / n) × ln 2)  clamped to [1, 30]
```

At the default 1% FP rate, this yields roughly **9.6 bits per key** and
**7 hash functions**.

## Double hashing

Rather than storing *k* independent hash functions, heliosDB uses the
**double-hashing** scheme:

```
h₁(key) = standard_hash(key)
h₂(key) = standard_hash(h₁)  |  1   ← ensure odd (avoids period ≠ num_bits)

bit_i = (h₁ + i × h₂) mod num_bits    for i in 0..num_hash_fns
```

This gives *k* pseudo-independent probes from two hash evaluations.

## Byte-boundary alignment

`num_bits` is always rounded up to a multiple of 8 before the bit array is
allocated:

```rust
let num_bits = (raw + 7) & !7;  // round up to byte boundary
```

Without this, the bit array would have `ceil(num_bits / 8)` bytes but the decoded
filter would compute `num_bits = bit_bytes * 8` — a mismatch that causes false
negatives (a bloom filter bug). The rounding ensures both sides agree exactly.

## Encoded format

```
[bit_array: num_bits/8 bytes]
[num_hash_fns: u8]
```

Total overhead per key: ~1.2 bytes at 1% FP rate (9.6 bits rounded to byte
boundary).

## API

```rust
// Building
let mut builder = BloomBuilder::new(expected_key_count);
for key in keys { builder.add(key); }
let encoded: Bytes = builder.finish();

// Reading
let filter = BloomFilter::decode(encoded).unwrap();

// Standard bloom check (for active segment membership)
if filter.may_contain(key) { ... }

// Negative existence check (for inactive segment skip)
if filter.definitely_not_here(key) {
    // skip this segment — key is provably absent
}
```

`definitely_not_here(key)` is simply `!may_contain(key)`. The naming makes the
intent clear at the call site.
"#
}

pub fn internals_wal() -> &'static str {
    r#"# Write-Ahead Log (WAL)

**Crate**: `heliosdb-engine`  **File**: `src/wal/mod.rs`

The WAL guarantees **crash durability**: every write that `put()` acknowledges
to the caller is recoverable even if the process dies before the MemTable is
flushed to the active segment.

## Record format

Each WAL record:

```
[record_type: u8]    0 = Full (entire entry in one record)
[data_len: u32]      length of the payload
[data: bytes]        encoded entry payload
[crc32: u32]         checksum of data bytes only
```

A full entry payload:

```
[key_len: u32]
[encoded_internal_key: bytes]   user_key + seq_num + op_type
[value_len: u32]
[value: bytes]
```

Fragmented records (First / Middle / Last) are supported by the format but
heliosDB currently produces only Full records. The type field is reserved for
future large-value fragmentation.

## Durability guarantee

After encoding the record, the WAL calls `fsync` (via `File::sync_data`) before
returning to the caller. This ensures the data has reached stable storage, not
just the OS page cache.

```rust
fn append(&mut self, key: &InternalKey, value: &Value) -> Result<()> {
    let payload = encode_entry(key, value);
    write_record(&mut self.writer, RECORD_FULL, &payload)?;
    self.writer.flush()?;        // flush BufWriter → OS
    self.writer.get_ref().sync_data()?;  // fsync → disk
    Ok(())
}
```

## Replay on open

`DB::open` calls `Wal::replay` before doing anything else:

```rust
Wal::replay(&wal_path, |key, value| {
    match key.op_type {
        OpType::Put    => memtable.put(key.user_key, key.seq_num, value),
        OpType::Delete => memtable.delete(key.user_key, key.seq_num),
    }
})?;
```

Replay stops at the first record with a CRC mismatch, treating it as
end-of-log (a partial write that occurred during a crash).

## WAL rotation

After a successful flush, the WAL is rotated: the old WAL file is replaced with
a new empty one. The flushed entries are now safe in the active segment and no
longer need WAL protection.

## File location

The WAL lives at `<db_dir>/WAL`. There is always at most one WAL file active.
"#
}

pub fn internals_memtable() -> &'static str {
    r#"# MemTable

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
"#
}

pub fn internals_compaction() -> &'static str {
    r#"# Compaction

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
"#
}
