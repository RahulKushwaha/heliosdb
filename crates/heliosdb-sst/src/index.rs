//! Index block: maps the last key of each data block → BlockHandle.
//!
//! Encoded format: repeated entries of
//! ```text
//! [key_len: u16][key bytes][block_handle: 12 bytes]
//! ```

use bytes::Bytes;
use heliosdb_types::{HeliosError, Result};

use crate::BlockHandle;

pub struct IndexBuilder {
    buf: Vec<u8>,
}

impl IndexBuilder {
    pub fn new() -> Self {
        Self { buf: Vec::new() }
    }

    /// Record that the data block ending at `last_key` is described by `handle`.
    pub fn add(&mut self, last_key: &[u8], handle: BlockHandle) {
        self.buf.extend_from_slice(&(last_key.len() as u16).to_le_bytes());
        self.buf.extend_from_slice(last_key);
        handle.encode_into(&mut self.buf);
    }

    pub fn finish(self) -> Bytes {
        Bytes::from(self.buf)
    }
}

impl Default for IndexBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// A decoded, read-only index block.
pub struct IndexBlock {
    entries: Vec<(Bytes, BlockHandle)>,
}

impl IndexBlock {
    pub fn decode(data: Bytes) -> Result<Self> {
        let mut entries = Vec::new();
        let mut pos = 0;
        while pos < data.len() {
            if pos + 2 > data.len() {
                return Err(HeliosError::Corruption("index entry key_len truncated".into()));
            }
            let key_len = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
            pos += 2;
            if pos + key_len + BlockHandle::ENCODED_SIZE > data.len() {
                return Err(HeliosError::Corruption("index entry truncated".into()));
            }
            let key = data.slice(pos..pos + key_len);
            pos += key_len;
            let handle = BlockHandle::decode(&data[pos..])
                .ok_or_else(|| HeliosError::Corruption("index BlockHandle truncated".into()))?;
            pos += BlockHandle::ENCODED_SIZE;
            entries.push((key, handle));
        }
        Ok(Self { entries })
    }

    /// Find the BlockHandle for the block that *might* contain `search_key`.
    ///
    /// Returns the first block whose last_key >= search_key (binary search).
    pub fn find(&self, search_key: &[u8]) -> Option<BlockHandle> {
        let idx = self.entries.partition_point(|(last_key, _)| last_key.as_ref() < search_key);
        self.entries.get(idx).map(|(_, h)| *h)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&[u8], BlockHandle)> {
        self.entries.iter().map(|(k, h)| (k.as_ref(), *h))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn find_correct_block() {
        let mut builder = IndexBuilder::new();
        builder.add(b"apple",  BlockHandle { offset: 0,   size: 100 });
        builder.add(b"mango",  BlockHandle { offset: 100, size: 200 });
        builder.add(b"zebra",  BlockHandle { offset: 300, size: 150 });
        let raw = builder.finish();
        let idx = IndexBlock::decode(raw).unwrap();

        assert_eq!(idx.find(b"cherry").unwrap().offset, 100); // between apple and mango → mango block
        assert_eq!(idx.find(b"apple").unwrap().offset, 0);
        assert_eq!(idx.find(b"zzz"), None); // past all blocks
    }
}
