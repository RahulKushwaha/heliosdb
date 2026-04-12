//! SST file builder.
//!
//! Writes entries (in sorted key order) to a file, producing:
//!   data blocks → bloom filter block → index block → properties block → footer

use std::io::Write;

use bytes::Bytes;
use heliosdb_types::Result;

use crate::{
    block::{BlockBuilder, CompressionType},
    bloom::BloomBuilder,
    index::IndexBuilder,
    BlockHandle, Footer, FOOTER_SIZE,
};

/// How large a data block can grow before being flushed (uncompressed estimate).
const BLOCK_SIZE_TARGET: usize = 64 * 1024; // 64 KiB

/// Metadata written into the properties block.
#[derive(Debug, Default)]
pub struct TableProperties {
    pub entry_count: u64,
    pub key_min:     Vec<u8>,
    pub key_max:     Vec<u8>,
    pub compression: CompressionType,
}

impl TableProperties {
    fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&self.entry_count.to_le_bytes());
        buf.extend_from_slice(&(self.key_min.len() as u32).to_le_bytes());
        buf.extend_from_slice(&self.key_min);
        buf.extend_from_slice(&(self.key_max.len() as u32).to_le_bytes());
        buf.extend_from_slice(&self.key_max);
        buf.push(self.compression as u8);
        buf
    }
}

pub struct SstBuilder<W: Write> {
    writer:       W,
    written:      u64,
    block:        BlockBuilder,
    bloom:        BloomBuilder,
    index:        IndexBuilder,
    props:        TableProperties,
    last_key:     Vec<u8>,
    expected_keys: usize,
    compression:  CompressionType,
}

impl<W: Write> SstBuilder<W> {
    pub fn new(writer: W, expected_keys: usize, compression: CompressionType) -> Self {
        Self {
            writer,
            written: 0,
            block: BlockBuilder::new(compression),
            bloom: BloomBuilder::new(expected_keys),
            index: IndexBuilder::new(),
            props: TableProperties { compression, ..Default::default() },
            last_key: Vec::new(),
            expected_keys,
            compression,
        }
    }

    /// Add a key-value pair. Keys must be in strictly ascending order.
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if self.props.key_min.is_empty() {
            self.props.key_min = key.to_vec();
        }
        self.props.key_max = key.to_vec();
        self.props.entry_count += 1;

        self.bloom.add(key);
        self.block.add(key, value);
        self.last_key = key.to_vec();

        if self.block.size_estimate() >= BLOCK_SIZE_TARGET {
            self.flush_block()?;
        }
        Ok(())
    }

    /// Finish writing the SST file. Flushes remaining data and writes the footer.
    pub fn finish(mut self) -> Result<u64> {
        if !self.block.is_empty() {
            self.flush_block()?;
        }

        // Destructure to avoid partial-move borrow conflicts.
        let SstBuilder { mut writer, mut written, bloom, index, props, .. } = self;

        let write_raw = |writer: &mut W, written: &mut u64, data: &[u8]| -> Result<BlockHandle> {
            let offset = *written;
            writer.write_all(data)?;
            let size = data.len() as u32;
            *written += size as u64;
            Ok(BlockHandle { offset, size })
        };

        let bloom_data  = bloom.finish();
        let bloom_handle = write_raw(&mut writer, &mut written, &bloom_data)?;

        let index_data  = index.finish();
        let index_handle = write_raw(&mut writer, &mut written, &index_data)?;

        let props_data  = props.encode();
        let props_handle = write_raw(&mut writer, &mut written, &props_data)?;

        let footer = Footer { bloom_handle, index_handle, props_handle };
        writer.write_all(&footer.encode())?;
        written += FOOTER_SIZE as u64;

        writer.flush()?;
        Ok(written)
    }

    fn flush_block(&mut self) -> Result<()> {
        let block = std::mem::replace(&mut self.block, BlockBuilder::new(self.compression));
        let encoded = block.finish()?;
        let handle = self.write_raw_block(&encoded)?;
        self.index.add(&self.last_key, handle);
        Ok(())
    }

    fn write_raw_block(&mut self, data: &[u8]) -> Result<BlockHandle> {
        let offset = self.written;
        self.writer.write_all(data)?;
        let size = data.len() as u32;
        self.written += size as u64;
        Ok(BlockHandle { offset, size })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_and_measure() {
        let mut buf = Vec::new();
        let mut b = SstBuilder::new(&mut buf, 100, CompressionType::None);
        for i in 0u32..100 {
            let key = format!("key{i:04}");
            let val = format!("value{i:04}");
            b.add(key.as_bytes(), val.as_bytes()).unwrap();
        }
        let total = b.finish().unwrap();
        assert_eq!(total, buf.len() as u64);
        assert!(buf.len() >= FOOTER_SIZE);
    }
}
