pub mod block;
pub mod bloom;
pub mod builder;
pub mod compression;
pub mod index;
pub mod reader;

pub use compression::CompressionType;

pub use builder::SstBuilder;
pub use reader::SstReader;

/// Magic number written in the SST footer: ASCII "HELIOSDB".
pub const MAGIC: u64 = 0x48454C494F534442;

/// Footer is always exactly 48 bytes at the end of every SST file.
pub const FOOTER_SIZE: usize = 48;

/// A (file_offset, byte_length) pointer to a block inside an SST file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockHandle {
    pub offset: u64,
    pub size:   u32,
}

impl BlockHandle {
    pub const ENCODED_SIZE: usize = 12; // 8 + 4

    pub fn encode_into(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.offset.to_le_bytes());
        buf.extend_from_slice(&self.size.to_le_bytes());
    }

    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.len() < Self::ENCODED_SIZE {
            return None;
        }
        let offset = u64::from_le_bytes(data[..8].try_into().unwrap());
        let size   = u32::from_le_bytes(data[8..12].try_into().unwrap());
        Some(Self { offset, size })
    }
}

/// Footer layout (48 bytes):
/// ```text
/// [bloom_handle: 12B][index_handle: 12B][props_handle: 12B][_pad: 4B][magic: 8B]
/// ```
#[derive(Debug, Clone)]
pub struct Footer {
    pub bloom_handle: BlockHandle,
    pub index_handle: BlockHandle,
    pub props_handle: BlockHandle,
}

impl Footer {
    pub fn encode(&self) -> [u8; FOOTER_SIZE] {
        let mut buf = Vec::with_capacity(FOOTER_SIZE);
        self.bloom_handle.encode_into(&mut buf);
        self.index_handle.encode_into(&mut buf);
        self.props_handle.encode_into(&mut buf);
        buf.extend_from_slice(&[0u8; 4]); // padding
        buf.extend_from_slice(&MAGIC.to_le_bytes());
        buf.try_into().unwrap()
    }

    pub fn decode(data: &[u8; FOOTER_SIZE]) -> Option<Self> {
        let magic = u64::from_le_bytes(data[40..48].try_into().unwrap());
        if magic != MAGIC {
            return None;
        }
        let bloom_handle = BlockHandle::decode(&data[0..])?;
        let index_handle = BlockHandle::decode(&data[12..])?;
        let props_handle = BlockHandle::decode(&data[24..])?;
        Some(Self { bloom_handle, index_handle, props_handle })
    }
}
