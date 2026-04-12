//! Write-Ahead Log (WAL).
//!
//! Each record:
//! ```text
//! [record_type: u8][data_len: u32][data: bytes][crc32: u32]
//! ```
//! record_type:
//!   0 = Full  (entire entry in one record)
//!   1 = First (start of a fragmented entry)
//!   2 = Middle
//!   3 = Last

use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Read, Write},
    path::{Path, PathBuf},
};

use heliosdb_types::{HeliosError, InternalKey, Result, Value};

const RECORD_FULL:   u8 = 0;
const RECORD_FIRST:  u8 = 1;
const RECORD_MIDDLE: u8 = 2;
const RECORD_LAST:   u8 = 3;

// ---------------------------------------------------------------------------
// Wal (writer)
// ---------------------------------------------------------------------------

pub struct Wal {
    path:   PathBuf,
    writer: BufWriter<File>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path.as_ref())?;
        Ok(Self { path: path.as_ref().to_path_buf(), writer: BufWriter::new(file) })
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path.as_ref())?;
        Ok(Self { path: path.as_ref().to_path_buf(), writer: BufWriter::new(file) })
    }

    /// Append a single key-value entry and fsync.
    pub fn append(&mut self, key: &InternalKey, value: &Value) -> Result<()> {
        let payload = encode_entry(key, value);
        write_record(&mut self.writer, RECORD_FULL, &payload)?;
        self.writer.flush()?;
        // fsync for durability
        self.writer.get_ref().sync_data()?;
        Ok(())
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Replay all records in the WAL file, calling `f` for each (key, value) pair.
    pub fn replay(path: impl AsRef<Path>, mut f: impl FnMut(InternalKey, Value)) -> Result<()> {
        let mut file = match File::open(path.as_ref()) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        let mut pos = 0;
        while pos < buf.len() {
            let (record_type, data, consumed) = read_record(&buf[pos..])
                .ok_or_else(|| HeliosError::Corruption("truncated WAL record".into()))?;
            pos += consumed;
            if record_type == RECORD_FULL {
                let (key, value) = decode_entry(&data)?;
                f(key, value);
            }
            // Fragmented records (FIRST/MIDDLE/LAST) are assembled here if needed.
            // For now we only produce FULL records, so this path is not exercised.
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Record encoding/decoding
// ---------------------------------------------------------------------------

fn write_record(w: &mut impl Write, record_type: u8, data: &[u8]) -> Result<()> {
    let crc = crc32fast::hash(data);
    w.write_all(&[record_type])?;
    w.write_all(&(data.len() as u32).to_le_bytes())?;
    w.write_all(data)?;
    w.write_all(&crc.to_le_bytes())?;
    Ok(())
}

/// Returns `(record_type, data, bytes_consumed)` or `None` on truncation.
fn read_record(buf: &[u8]) -> Option<(u8, Vec<u8>, usize)> {
    if buf.len() < 9 {
        return None;
    }
    let record_type = buf[0];
    let data_len = u32::from_le_bytes(buf[1..5].try_into().unwrap()) as usize;
    let total = 1 + 4 + data_len + 4;
    if buf.len() < total {
        return None;
    }
    let data = buf[5..5 + data_len].to_vec();
    let stored_crc = u32::from_le_bytes(buf[5 + data_len..total].try_into().unwrap());
    let actual_crc = crc32fast::hash(&data);
    if stored_crc != actual_crc {
        return None; // treat corruption as end-of-log
    }
    Some((record_type, data, total))
}

/// WAL entry payload: `[key_len: u32][encoded_internal_key][value_len: u32][value]`
fn encode_entry(key: &InternalKey, value: &Value) -> Vec<u8> {
    let enc_key = key.encode();
    let mut buf = Vec::with_capacity(8 + enc_key.len() + value.len());
    buf.extend_from_slice(&(enc_key.len() as u32).to_le_bytes());
    buf.extend_from_slice(&enc_key);
    buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
    buf.extend_from_slice(value);
    buf
}

fn decode_entry(data: &[u8]) -> Result<(InternalKey, Value)> {
    if data.len() < 8 {
        return Err(HeliosError::Corruption("WAL entry too short".into()));
    }
    let key_len = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
    if data.len() < 4 + key_len + 4 {
        return Err(HeliosError::Corruption("WAL entry key truncated".into()));
    }
    let key = InternalKey::decode(&data[4..4 + key_len])?;
    let val_start = 4 + key_len;
    let val_len = u32::from_le_bytes(data[val_start..val_start + 4].try_into().unwrap()) as usize;
    let val_end = val_start + 4 + val_len;
    if data.len() < val_end {
        return Err(HeliosError::Corruption("WAL entry value truncated".into()));
    }
    let value = bytes::Bytes::copy_from_slice(&data[val_start + 4..val_end]);
    Ok((key, value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use heliosdb_types::OpType;
    use tempfile::NamedTempFile;

    #[test]
    fn roundtrip() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        let entries: Vec<(InternalKey, Value)> = (0u64..50)
            .map(|i| {
                (
                    InternalKey::new_put(Bytes::from(format!("key{i:04}")), i),
                    Bytes::from(format!("val{i}")),
                )
            })
            .collect();

        {
            let mut wal = Wal::create(&path).unwrap();
            for (k, v) in &entries {
                wal.append(k, v).unwrap();
            }
        }

        let mut recovered = Vec::new();
        Wal::replay(&path, |k, v| recovered.push((k, v))).unwrap();
        assert_eq!(recovered.len(), 50);
        assert_eq!(recovered[0].0.user_key, Bytes::from("key0000"));
        assert_eq!(recovered[49].0.op_type, OpType::Put);
    }

    #[test]
    fn replay_missing_file_is_ok() {
        Wal::replay("/tmp/helios_nonexistent_wal_test.log", |_, _| {}).unwrap();
    }
}
