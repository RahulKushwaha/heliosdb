# Bloom Filter

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
