mod error;
mod key;

pub use error::HeliosError;
pub use key::{InternalKey, OpType, UserKey, Value};

pub type Result<T> = std::result::Result<T, HeliosError>;

/// Monotonically increasing write sequence number.
pub type SeqNum = u64;

/// Maximum valid sequence number (top 7 bytes of a u64).
pub const MAX_SEQ_NUM: SeqNum = (1 << 56) - 1;
