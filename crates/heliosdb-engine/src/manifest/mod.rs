//! Manifest — append-only log of version edits.
//!
//! Each edit records:
//!   - files added to a level
//!   - files removed from a level
//!   - the current active segment path
//!
//! On open, the manifest is replayed to reconstruct the current VersionSet.

use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Read, Write},
    path::{Path, PathBuf},
};

use heliosdb_types::{HeliosError, Result};

pub mod version;
pub use version::VersionSet;

// ---------------------------------------------------------------------------
// Edit types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum Edit {
    /// A new inactive segment was added at the given level.
    AddInactive { level: u32, path: PathBuf },
    /// An inactive segment was removed (after compaction).
    RemoveInactive { level: u32, path: PathBuf },
    /// The active segment path changed (after a flush).
    SetActive { path: PathBuf },
    /// The next sequence number to use.
    SetNextSeq { seq: u64 },
}

// ---------------------------------------------------------------------------
// Manifest (writer + recovery)
// ---------------------------------------------------------------------------

pub struct Manifest {
    path:   PathBuf,
    writer: BufWriter<File>,
}

impl Manifest {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path.as_ref())?;
        Ok(Self { path: path.as_ref().to_path_buf(), writer: BufWriter::new(file) })
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path.as_ref())?;
        Ok(Self { path: path.as_ref().to_path_buf(), writer: BufWriter::new(file) })
    }

    /// Append an edit and fsync.
    pub fn append(&mut self, edit: &Edit) -> Result<()> {
        let encoded = encode_edit(edit);
        let crc = crc32fast::hash(&encoded);
        self.writer.write_all(&(encoded.len() as u32).to_le_bytes())?;
        self.writer.write_all(&encoded)?;
        self.writer.write_all(&crc.to_le_bytes())?;
        self.writer.flush()?;
        self.writer.get_ref().sync_data()?;
        Ok(())
    }

    /// Replay the manifest and return the recovered VersionSet.
    pub fn recover(path: impl AsRef<Path>) -> Result<VersionSet> {
        let mut vs = VersionSet::default();
        let mut file = match File::open(path.as_ref()) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(vs),
            Err(e) => return Err(e.into()),
        };
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        let mut pos = 0;
        while pos < buf.len() {
            if pos + 4 > buf.len() {
                break; // truncated length
            }
            let len = u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            if pos + len + 4 > buf.len() {
                break; // truncated data
            }
            let data = &buf[pos..pos + len];
            let stored_crc = u32::from_le_bytes(buf[pos + len..pos + len + 4].try_into().unwrap());
            let actual_crc = crc32fast::hash(data);
            if stored_crc != actual_crc {
                break; // treat as end of valid log
            }
            pos += len + 4;

            if let Ok(edit) = decode_edit(data) {
                vs.apply(&edit);
            }
        }
        Ok(vs)
    }
}

// ---------------------------------------------------------------------------
// Edit encoding (simple tag-length-value)
// ---------------------------------------------------------------------------

const TAG_ADD_INACTIVE:    u8 = 1;
const TAG_REMOVE_INACTIVE: u8 = 2;
const TAG_SET_ACTIVE:      u8 = 3;
const TAG_SET_NEXT_SEQ:    u8 = 4;

fn encode_edit(edit: &Edit) -> Vec<u8> {
    match edit {
        Edit::AddInactive { level, path } => {
            let p = path.to_string_lossy();
            let mut b = vec![TAG_ADD_INACTIVE];
            b.extend_from_slice(&level.to_le_bytes());
            b.extend_from_slice(&(p.len() as u32).to_le_bytes());
            b.extend_from_slice(p.as_bytes());
            b
        }
        Edit::RemoveInactive { level, path } => {
            let p = path.to_string_lossy();
            let mut b = vec![TAG_REMOVE_INACTIVE];
            b.extend_from_slice(&level.to_le_bytes());
            b.extend_from_slice(&(p.len() as u32).to_le_bytes());
            b.extend_from_slice(p.as_bytes());
            b
        }
        Edit::SetActive { path } => {
            let p = path.to_string_lossy();
            let mut b = vec![TAG_SET_ACTIVE];
            b.extend_from_slice(&(p.len() as u32).to_le_bytes());
            b.extend_from_slice(p.as_bytes());
            b
        }
        Edit::SetNextSeq { seq } => {
            let mut b = vec![TAG_SET_NEXT_SEQ];
            b.extend_from_slice(&seq.to_le_bytes());
            b
        }
    }
}

fn decode_edit(data: &[u8]) -> Result<Edit> {
    if data.is_empty() {
        return Err(HeliosError::Corruption("empty edit".into()));
    }
    match data[0] {
        TAG_ADD_INACTIVE | TAG_REMOVE_INACTIVE => {
            let level = u32::from_le_bytes(data[1..5].try_into().unwrap());
            let plen = u32::from_le_bytes(data[5..9].try_into().unwrap()) as usize;
            let path = PathBuf::from(std::str::from_utf8(&data[9..9 + plen]).unwrap());
            if data[0] == TAG_ADD_INACTIVE {
                Ok(Edit::AddInactive { level, path })
            } else {
                Ok(Edit::RemoveInactive { level, path })
            }
        }
        TAG_SET_ACTIVE => {
            let plen = u32::from_le_bytes(data[1..5].try_into().unwrap()) as usize;
            let path = PathBuf::from(std::str::from_utf8(&data[5..5 + plen]).unwrap());
            Ok(Edit::SetActive { path })
        }
        TAG_SET_NEXT_SEQ => {
            let seq = u64::from_le_bytes(data[1..9].try_into().unwrap());
            Ok(Edit::SetNextSeq { seq })
        }
        t => Err(HeliosError::Corruption(format!("unknown edit tag {t}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn roundtrip_edits() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path();

        {
            let mut m = Manifest::create(path).unwrap();
            m.append(&Edit::SetActive { path: PathBuf::from("/db/active.sst") }).unwrap();
            m.append(&Edit::AddInactive { level: 1, path: PathBuf::from("/db/l1_001.sst") }).unwrap();
            m.append(&Edit::SetNextSeq { seq: 42 }).unwrap();
        }

        let vs = Manifest::recover(path).unwrap();
        assert_eq!(vs.active_path(), Some(Path::new("/db/active.sst")));
        assert_eq!(vs.inactive_at_level(1).len(), 1);
        assert_eq!(vs.next_seq(), 42);
    }
}
