use heliosdb_types::{HeliosError, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CompressionType {
    None   = 0,
    Snappy = 1,
    Zstd   = 2,
}

impl Default for CompressionType {
    fn default() -> Self {
        CompressionType::None
    }
}

impl TryFrom<u8> for CompressionType {
    type Error = HeliosError;
    fn try_from(v: u8) -> Result<Self> {
        match v {
            0 => Ok(Self::None),
            1 => Ok(Self::Snappy),
            2 => Ok(Self::Zstd),
            _ => Err(HeliosError::Corruption(format!("unknown compression type {v}"))),
        }
    }
}

pub fn compress(data: &[u8], kind: CompressionType) -> Result<Vec<u8>> {
    match kind {
        CompressionType::None => Ok(data.to_vec()),
        CompressionType::Snappy => {
            let mut enc = snap::raw::Encoder::new();
            enc.compress_vec(data)
                .map_err(|e| HeliosError::Compression(e.to_string()))
        }
        CompressionType::Zstd => zstd::encode_all(data, 3)
            .map_err(|e| HeliosError::Compression(e.to_string())),
    }
}

pub fn decompress(data: &[u8], kind: CompressionType) -> Result<Vec<u8>> {
    match kind {
        CompressionType::None => Ok(data.to_vec()),
        CompressionType::Snappy => {
            let mut dec = snap::raw::Decoder::new();
            dec.decompress_vec(data)
                .map_err(|e| HeliosError::Compression(e.to_string()))
        }
        CompressionType::Zstd => zstd::decode_all(data)
            .map_err(|e| HeliosError::Compression(e.to_string())),
    }
}
