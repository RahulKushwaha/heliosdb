//! SST file reader.
//!
//! Supports:
//!   - Point lookup: bloom filter → index → block decode → linear scan
//!   - Sequential scan: iterate all data blocks in order

use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::Path,
};

use bytes::Bytes;
use heliosdb_types::{HeliosError, Result};

use crate::{
    block::{BlockDecoder, CompressionType},
    bloom::BloomFilter,
    index::IndexBlock,
    BlockHandle, Footer, FOOTER_SIZE,
};

pub struct SstReader {
    file:         File,
    file_size:    u64,
    filter:       BloomFilter,
    index:        IndexBlock,
    _compression: CompressionType,
}

impl SstReader {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let mut file = File::open(path)?;
        let file_size = file.metadata()?.len();

        if file_size < FOOTER_SIZE as u64 {
            return Err(HeliosError::Corruption("file too small for footer".into()));
        }

        // Read footer
        file.seek(SeekFrom::Start(file_size - FOOTER_SIZE as u64))?;
        let mut footer_buf = [0u8; FOOTER_SIZE];
        file.read_exact(&mut footer_buf)?;
        let footer = Footer::decode(&footer_buf)
            .ok_or_else(|| HeliosError::Corruption("bad footer magic".into()))?;

        // Read and decode bloom filter block
        let bloom_data = read_block_raw(&mut file, footer.bloom_handle)?;
        let filter = BloomFilter::decode(bloom_data)
            .ok_or_else(|| HeliosError::Corruption("empty bloom filter".into()))?;

        // Read and decode index block
        let index_data = read_block_raw(&mut file, footer.index_handle)?;
        let index = IndexBlock::decode(index_data)?;

        Ok(Self {
            file,
            file_size,
            filter,
            index,
            _compression: CompressionType::None, // stored in props; decoder auto-detects per block
        })
    }

    /// Lookup `key`. Returns `None` if definitively absent (bloom miss).
    /// Returns `Some(None)` if bloom passed but key was not found in the block.
    /// Returns `Some(Some(value))` on hit.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Bytes>> {
        if self.filter.definitely_not_here(key) {
            return Ok(None);
        }
        let Some(handle) = self.index.find(key) else {
            return Ok(None);
        };
        let block = self.read_block(handle)?;
        Ok(block.seek(key).map(|(_, v)| v))
    }

    /// Returns an iterator that streams all (key, value) pairs in the SST in order.
    pub fn iter(&mut self) -> Result<SstIter> {
        let handles: Vec<BlockHandle> = self.index.iter().map(|(_, h)| h).collect();
        let mut blocks = Vec::with_capacity(handles.len());
        for h in handles {
            blocks.push(self.read_block(h)?);
        }
        Ok(SstIter { blocks, block_idx: 0, inner: None })
    }

    fn read_block(&mut self, handle: BlockHandle) -> Result<BlockDecoder> {
        let raw = read_block_raw(&mut self.file, handle)?;
        BlockDecoder::decode(raw)
    }

    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    /// Negative existence filter — `true` means the key is definitely NOT in this file.
    pub fn definitely_not_here(&self, key: &[u8]) -> bool {
        self.filter.definitely_not_here(key)
    }
}

fn read_block_raw(file: &mut File, handle: BlockHandle) -> Result<Bytes> {
    file.seek(SeekFrom::Start(handle.offset))?;
    let mut buf = vec![0u8; handle.size as usize];
    file.read_exact(&mut buf)?;
    Ok(Bytes::from(buf))
}

// ---------------------------------------------------------------------------
// SstIter
// ---------------------------------------------------------------------------

pub struct SstIter {
    blocks:    Vec<BlockDecoder>,
    block_idx: usize,
    inner:     Option<crate::block::BlockIter>,
}

impl Iterator for SstIter {
    type Item = Result<(Bytes, Bytes)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(ref mut it) = self.inner {
                if let Some(pair) = it.next() {
                    return Some(Ok(pair));
                }
            }
            // Advance to next block
            if self.block_idx >= self.blocks.len() {
                return None;
            }
            let iter = self.blocks[self.block_idx].iter();
            self.block_idx += 1;
            self.inner = Some(iter);
        }
    }
}
