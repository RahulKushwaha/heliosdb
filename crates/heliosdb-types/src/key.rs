use bytes::Bytes;

use crate::{HeliosError, Result, SeqNum, MAX_SEQ_NUM};

/// Raw user-provided key.
pub type UserKey = Bytes;

/// Raw user-provided value.
pub type Value = Bytes;

/// Operation type encoded in the low byte of an InternalKey's trailer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum OpType {
    Put    = 0,
    Delete = 1,
}

impl TryFrom<u8> for OpType {
    type Error = HeliosError;

    fn try_from(v: u8) -> Result<Self> {
        match v {
            0 => Ok(OpType::Put),
            1 => Ok(OpType::Delete),
            _ => Err(HeliosError::Corruption(format!("unknown op_type {v}"))),
        }
    }
}

/// Internal key layout: `[user_key][seq_num (7 bytes, BE)][op_type (1 byte)]`
///
/// Stored in SST blocks and the MemTable. Ordering is:
///   - ascending by user_key
///   - descending by seq_num (so the latest version sorts first)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InternalKey {
    pub user_key: UserKey,
    pub seq_num:  SeqNum,
    pub op_type:  OpType,
}

/// Encoded length overhead added to the user key (7-byte seq + 1-byte op).
pub const TRAILER_LEN: usize = 8;

impl InternalKey {
    pub fn new_put(user_key: UserKey, seq_num: SeqNum) -> Self {
        Self { user_key, seq_num, op_type: OpType::Put }
    }

    pub fn new_delete(user_key: UserKey, seq_num: SeqNum) -> Self {
        Self { user_key, seq_num, op_type: OpType::Delete }
    }

    /// Encode into `buf`.  Format: user_key bytes + 7-byte big-endian seq_num + 1-byte op_type.
    pub fn encode_into(&self, buf: &mut Vec<u8>) {
        assert!(self.seq_num <= MAX_SEQ_NUM, "seq_num exceeds 56-bit max");
        buf.extend_from_slice(&self.user_key);
        // Pack seq_num (7 bytes, big-endian) + op_type (1 byte) into a u64 trailer.
        let trailer = (self.seq_num << 8) | (self.op_type as u64);
        buf.extend_from_slice(&trailer.to_be_bytes());
    }

    /// Encode into a freshly allocated `Vec<u8>`.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.user_key.len() + TRAILER_LEN);
        self.encode_into(&mut buf);
        buf
    }

    /// Decode from an encoded byte slice.
    pub fn decode(data: &[u8]) -> Result<Self> {
        if data.len() < TRAILER_LEN {
            return Err(HeliosError::Corruption(
                "internal key too short".to_string(),
            ));
        }
        let (key_bytes, trailer_bytes) = data.split_at(data.len() - TRAILER_LEN);
        let trailer = u64::from_be_bytes(trailer_bytes.try_into().unwrap());
        let seq_num = trailer >> 8;
        let op_type = OpType::try_from((trailer & 0xff) as u8)?;
        Ok(Self {
            user_key: Bytes::copy_from_slice(key_bytes),
            seq_num,
            op_type,
        })
    }
}

impl PartialOrd for InternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for InternalKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Primary: ascending user_key
        // Secondary: descending seq_num (latest version sorts first)
        self.user_key
            .cmp(&other.user_key)
            .then_with(|| other.seq_num.cmp(&self.seq_num))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_put() {
        let key = InternalKey::new_put(Bytes::from("hello"), 42);
        let encoded = key.encode();
        let decoded = InternalKey::decode(&encoded).unwrap();
        assert_eq!(key, decoded);
    }

    #[test]
    fn roundtrip_delete() {
        let key = InternalKey::new_delete(Bytes::from("world"), 999);
        let decoded = InternalKey::decode(&key.encode()).unwrap();
        assert_eq!(key, decoded);
    }

    #[test]
    fn ordering_latest_first() {
        let a = InternalKey::new_put(Bytes::from("k"), 10);
        let b = InternalKey::new_put(Bytes::from("k"), 5);
        // a has higher seq_num, so it sorts before b (latest first)
        assert!(a < b);
    }
}
