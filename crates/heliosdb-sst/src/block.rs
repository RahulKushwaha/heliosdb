//! Data block encoder and decoder.
//!
//! Block layout (all little-endian):
//! ```text
//! [entries...]
//! [restart_offsets: u32 * num_restarts]
//! [num_restarts: u32]
//! [crc32: u32]
//! [compression_type: u8]
//! ```
//!
//! Each entry uses prefix compression:
//! ```text
//! [shared_key_len: u16][unshared_key_len: u16][value_len: u32][key_delta][value]
//! ```
//! A restart point resets shared_key_len to 0, enabling binary search.

use bytes::Bytes;
use heliosdb_types::{HeliosError, Result};

use crate::compression::{compress, decompress};
pub use crate::compression::CompressionType;

/// Number of entries between restart points.
const RESTART_INTERVAL: usize = 16;

// ---------------------------------------------------------------------------
// BlockBuilder
// ---------------------------------------------------------------------------

pub struct BlockBuilder {
    buf:             Vec<u8>,
    restart_offsets: Vec<u32>,
    last_key:        Vec<u8>,
    entry_count:     usize,
    compression:     CompressionType,
}

impl BlockBuilder {
    pub fn new(compression: CompressionType) -> Self {
        Self {
            buf: Vec::new(),
            restart_offsets: Vec::new(),
            last_key: Vec::new(),
            entry_count: 0,
            compression,
        }
    }

    /// Add a key-value entry. Keys must be added in sorted (ascending) order.
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        let shared = if self.entry_count % RESTART_INTERVAL == 0 {
            self.restart_offsets.push(self.buf.len() as u32);
            0
        } else {
            shared_prefix_len(&self.last_key, key)
        };

        let unshared = key.len() - shared;
        self.buf.extend_from_slice(&(shared as u16).to_le_bytes());
        self.buf.extend_from_slice(&(unshared as u16).to_le_bytes());
        self.buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
        self.buf.extend_from_slice(&key[shared..]);
        self.buf.extend_from_slice(value);

        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.entry_count += 1;
    }

    pub fn is_empty(&self) -> bool {
        self.entry_count == 0
    }

    pub fn size_estimate(&self) -> usize {
        self.buf.len() + self.restart_offsets.len() * 4 + 4 + 4 + 1
    }

    /// Finish the block: append restarts, CRC, compression type; return encoded bytes.
    pub fn finish(self) -> Result<Bytes> {
        let mut body = self.buf;
        for offset in &self.restart_offsets {
            body.extend_from_slice(&offset.to_le_bytes());
        }
        body.extend_from_slice(&(self.restart_offsets.len() as u32).to_le_bytes());

        let compressed = compress(&body, self.compression)?;
        let crc = crc32fast::hash(&compressed);

        let mut out = compressed;
        out.extend_from_slice(&crc.to_le_bytes());
        out.push(self.compression as u8);

        Ok(Bytes::from(out))
    }
}

// ---------------------------------------------------------------------------
// BlockDecoder
// ---------------------------------------------------------------------------

pub struct BlockDecoder {
    data:            Bytes, // decompressed entries + restart table
    restart_offsets: Vec<u32>,
}

impl BlockDecoder {
    /// Parse and decompress raw block bytes (as stored on disk).
    pub fn decode(raw: Bytes) -> Result<Self> {
        if raw.len() < 5 {
            return Err(HeliosError::Corruption("block too short".into()));
        }
        let compression_type_byte = *raw.last().unwrap();
        let compression = CompressionType::try_from(compression_type_byte)?;
        let crc_start = raw.len() - 5;
        let stored_crc = u32::from_le_bytes(raw[crc_start..crc_start + 4].try_into().unwrap());
        let compressed = &raw[..crc_start];

        let actual_crc = crc32fast::hash(compressed);
        if actual_crc != stored_crc {
            return Err(HeliosError::Corruption(format!(
                "block CRC mismatch: stored={stored_crc:#010x} actual={actual_crc:#010x}"
            )));
        }

        let data = decompress(compressed, compression)?;

        if data.len() < 4 {
            return Err(HeliosError::Corruption("block body too short".into()));
        }
        let num_restarts_offset = data.len() - 4;
        let num_restarts =
            u32::from_le_bytes(data[num_restarts_offset..].try_into().unwrap()) as usize;

        let restart_table_offset = num_restarts_offset
            .checked_sub(num_restarts * 4)
            .ok_or_else(|| HeliosError::Corruption("restart table overflows block".into()))?;

        let mut restart_offsets = Vec::with_capacity(num_restarts);
        for i in 0..num_restarts {
            let start = restart_table_offset + i * 4;
            let offset = u32::from_le_bytes(data[start..start + 4].try_into().unwrap());
            restart_offsets.push(offset);
        }

        let entries_end = restart_table_offset;
        Ok(Self {
            data: Bytes::copy_from_slice(&data[..entries_end]),
            restart_offsets,
        })
    }

    /// Return an iterator over all (key, value) pairs in the block (in order).
    pub fn iter(&self) -> BlockIter {
        BlockIter::new(self.data.clone(), self.restart_offsets.clone())
    }

    /// Seek to the first entry whose key >= `target`. Returns None if no such entry exists.
    pub fn seek(&self, target: &[u8]) -> Option<(Bytes, Bytes)> {
        // Binary search over restart points.
        let rp = self.restart_offsets.as_slice();
        let idx = rp.partition_point(|&off| {
            self.restart_key(off as usize)
                .map_or(false, |k| k.as_ref() < target)
        });
        // Start iteration from the restart point just before the target.
        let start_rp = idx.saturating_sub(1);
        let start_off = rp.get(start_rp).copied().unwrap_or(0) as usize;

        let mut iter = BlockIter::at(&self.data, start_off);
        while let Some((k, v)) = iter.next() {
            if k.as_ref() >= target {
                return Some((k, v));
            }
        }
        None
    }

    fn restart_key(&self, offset: usize) -> Option<Bytes> {
        // At a restart point, shared_len is always 0.
        if offset + 8 > self.data.len() {
            return None;
        }
        let unshared = u16::from_le_bytes(self.data[offset + 2..offset + 4].try_into().unwrap());
        let key_start = offset + 8;
        let key_end = key_start + unshared as usize;
        if key_end > self.data.len() {
            return None;
        }
        Some(self.data.slice(key_start..key_end))
    }
}

// ---------------------------------------------------------------------------
// BlockIter
// ---------------------------------------------------------------------------

pub struct BlockIter {
    data:     Bytes,
    pos:      usize,
    last_key: Vec<u8>,
}

impl BlockIter {
    fn new(data: Bytes, _restart_offsets: Vec<u32>) -> Self {
        Self { data, pos: 0, last_key: Vec::new() }
    }

    fn at(data: &Bytes, offset: usize) -> Self {
        Self { data: data.clone(), pos: offset, last_key: Vec::new() }
    }
}

impl Iterator for BlockIter {
    type Item = (Bytes, Bytes);

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.data.len() {
            return None;
        }
        if self.pos + 8 > self.data.len() {
            return None;
        }
        let shared   = u16::from_le_bytes(self.data[self.pos..self.pos + 2].try_into().unwrap());
        let unshared = u16::from_le_bytes(self.data[self.pos + 2..self.pos + 4].try_into().unwrap());
        let val_len  = u32::from_le_bytes(self.data[self.pos + 4..self.pos + 8].try_into().unwrap());
        self.pos += 8;

        let key_end = self.pos + unshared as usize;
        let val_end = key_end + val_len as usize;
        if val_end > self.data.len() {
            return None;
        }

        let mut full_key = self.last_key[..shared as usize].to_vec();
        full_key.extend_from_slice(&self.data[self.pos..key_end]);
        self.pos = val_end;

        let value = self.data.slice(key_end..val_end);
        self.last_key = full_key.clone();
        Some((Bytes::from(full_key), value))
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn shared_prefix_len(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
}


#[cfg(test)]
mod tests {
    use super::*;

    fn build_block(pairs: &[(&[u8], &[u8])]) -> Bytes {
        let mut b = BlockBuilder::new(CompressionType::None);
        for (k, v) in pairs {
            b.add(k, v);
        }
        b.finish().unwrap()
    }

    #[test]
    fn roundtrip_empty() {
        let raw = build_block(&[]);
        let dec = BlockDecoder::decode(raw).unwrap();
        assert_eq!(dec.iter().count(), 0);
    }

    #[test]
    fn roundtrip_entries() {
        let pairs: Vec<(&[u8], &[u8])> = vec![
            (b"apple", b"1"),
            (b"apricot", b"2"),
            (b"banana", b"3"),
            (b"cherry", b"4"),
        ];
        let raw = build_block(&pairs);
        let dec = BlockDecoder::decode(raw).unwrap();
        let got: Vec<_> = dec.iter().collect();
        assert_eq!(got.len(), 4);
        assert_eq!(got[0].0.as_ref(), b"apple");
        assert_eq!(got[2].1.as_ref(), b"3");
    }

    #[test]
    fn seek_found() {
        let pairs: Vec<(&[u8], &[u8])> = (0u32..100)
            .map(|i| (format!("key{i:04}"), format!("val{i:04}")))
            .collect::<Vec<_>>()
            .iter()
            .map(|(k, v)| (k.as_bytes(), v.as_bytes()))
            .collect::<Vec<_>>();
        // Can't use temp strings directly — build owned vecs
        let owned: Vec<(Vec<u8>, Vec<u8>)> = (0u32..100)
            .map(|i| (format!("key{i:04}").into_bytes(), format!("val{i:04}").into_bytes()))
            .collect();
        let mut b = BlockBuilder::new(CompressionType::None);
        for (k, v) in &owned {
            b.add(k, v);
        }
        let raw = b.finish().unwrap();
        let dec = BlockDecoder::decode(raw).unwrap();
        let (k, v) = dec.seek(b"key0050").unwrap();
        assert_eq!(k.as_ref(), b"key0050");
        assert_eq!(v.as_ref(), b"val0050");
    }

    #[test]
    fn seek_past_end_returns_none() {
        let raw = build_block(&[(b"aaa", b"1"), (b"bbb", b"2")]);
        let dec = BlockDecoder::decode(raw).unwrap();
        assert!(dec.seek(b"zzz").is_none());
    }

    #[test]
    fn roundtrip_snappy() {
        let mut b = BlockBuilder::new(CompressionType::Snappy);
        for i in 0..50u32 {
            b.add(format!("k{i:03}").as_bytes(), format!("value{i}").as_bytes());
        }
        let raw = b.finish().unwrap();
        let dec = BlockDecoder::decode(raw).unwrap();
        assert_eq!(dec.iter().count(), 50);
    }
}
