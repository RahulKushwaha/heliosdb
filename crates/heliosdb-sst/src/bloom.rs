//! Bloom filter block.
//!
//! Uses double hashing: `h_i(x) = h1(x) + i * h2(x)`.
//! The filter can be used in two modes:
//!   - **Positive** (active segment): `may_contain(k)` → true means "possibly here"
//!   - **Negative** (inactive segment): `definitely_not_here(k)` → true means "skip this segment"
//!
//! Encoded format:
//! ```text
//! [bit_array: ceil(n_bits / 8) bytes][num_hash_fns: u8]
//! ```

use bytes::Bytes;

/// Target false-positive rate used to size the bit array.
const TARGET_FP_RATE: f64 = 0.01; // 1 %

pub struct BloomBuilder {
    bits:        Vec<u8>,
    num_hash_fns: u8,
    num_bits:    usize,
}

impl BloomBuilder {
    /// Create a builder sized for `expected_keys` keys at ~1% FP rate.
    pub fn new(expected_keys: usize) -> Self {
        // Optimal bit count: n * -ln(p) / (ln2)^2
        let num_bits = if expected_keys == 0 {
            64
        } else {
            let p = TARGET_FP_RATE;
            let n = expected_keys as f64;
            let raw = ((n * (-p.ln()) / std::f64::consts::LN_2.powi(2)).ceil() as usize).max(64);
            // Round up to a byte boundary so that bit_bytes*8 == num_bits exactly on decode.
            (raw + 7) & !7
        };
        // Optimal k: (m/n) * ln2
        let num_hash_fns = if expected_keys == 0 {
            6
        } else {
            let k = (num_bits as f64 / expected_keys as f64 * std::f64::consts::LN_2)
                .round() as usize;
            k.clamp(1, 30) as u8
        };
        let byte_count = (num_bits + 7) / 8;
        Self {
            bits: vec![0u8; byte_count],
            num_hash_fns,
            num_bits,
        }
    }

    pub fn add(&mut self, key: &[u8]) {
        let (h1, h2) = double_hash(key);
        for i in 0..self.num_hash_fns as u64 {
            let bit = (h1.wrapping_add(i.wrapping_mul(h2)) % self.num_bits as u64) as usize;
            self.bits[bit / 8] |= 1 << (bit % 8);
        }
    }

    pub fn finish(mut self) -> Bytes {
        self.bits.push(self.num_hash_fns);
        Bytes::from(self.bits)
    }
}

/// A decoded, read-only Bloom filter.
#[derive(Clone)]
pub struct BloomFilter {
    bits:        Bytes,
    num_hash_fns: u8,
    num_bits:    usize,
}

impl BloomFilter {
    pub fn decode(data: Bytes) -> Option<Self> {
        if data.is_empty() {
            return None;
        }
        let num_hash_fns = *data.last().unwrap();
        let bit_bytes = data.len() - 1;
        Some(Self {
            num_bits: bit_bytes * 8,
            bits: data.slice(..bit_bytes),
            num_hash_fns,
        })
    }

    /// Returns `true` if the key *might* be in the set (standard bloom semantics).
    pub fn may_contain(&self, key: &[u8]) -> bool {
        if self.num_bits == 0 {
            return true;
        }
        let (h1, h2) = double_hash(key);
        for i in 0..self.num_hash_fns as u64 {
            let bit = (h1.wrapping_add(i.wrapping_mul(h2)) % self.num_bits as u64) as usize;
            if self.bits[bit / 8] & (1 << (bit % 8)) == 0 {
                return false;
            }
        }
        true
    }

    /// Convenience: returns `true` if the key is *definitely absent* — used as the
    /// negative existence filter on inactive segments.
    #[inline]
    pub fn definitely_not_here(&self, key: &[u8]) -> bool {
        !self.may_contain(key)
    }
}

fn double_hash(key: &[u8]) -> (u64, u64) {
    use std::hash::{DefaultHasher, Hash, Hasher};

    // h1: standard hash
    let mut h = DefaultHasher::new();
    key.hash(&mut h);
    let h1 = h.finish();

    // h2: hash of the hash (cheap second independent hash)
    let mut h = DefaultHasher::new();
    h1.hash(&mut h);
    let h2 = h.finish() | 1; // ensure odd so GCD(h2, num_bits) can be 1
    (h1, h2)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_false_negatives() {
        let keys: Vec<Vec<u8>> = (0u32..1000)
            .map(|i| format!("key{i:06}").into_bytes())
            .collect();

        let mut builder = BloomBuilder::new(keys.len());
        for k in &keys {
            builder.add(k);
        }
        let raw = builder.finish();
        let filter = BloomFilter::decode(raw).unwrap();

        for k in &keys {
            assert!(filter.may_contain(k), "false negative for {k:?}");
        }
    }

    #[test]
    fn false_positive_rate_within_bound() {
        let n = 10_000usize;
        let mut builder = BloomBuilder::new(n);
        for i in 0..n {
            builder.add(format!("present{i}").as_bytes());
        }
        let raw = builder.finish();
        let filter = BloomFilter::decode(raw).unwrap();

        let mut fp = 0usize;
        let probes = 100_000usize;
        for i in 0..probes {
            if filter.may_contain(format!("absent{i}").as_bytes()) {
                fp += 1;
            }
        }
        let fp_rate = fp as f64 / probes as f64;
        // Allow generous headroom: 5% (should be ~1%)
        assert!(fp_rate < 0.05, "FP rate too high: {fp_rate:.4}");
    }
}
