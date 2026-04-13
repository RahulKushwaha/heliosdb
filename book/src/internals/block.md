# Block Encoder / Decoder

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
