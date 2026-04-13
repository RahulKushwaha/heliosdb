# File Format

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
