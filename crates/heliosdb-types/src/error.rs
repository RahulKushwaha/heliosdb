use thiserror::Error;

#[derive(Debug, Error)]
pub enum HeliosError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("corruption: {0}")]
    Corruption(String),

    #[error("key not found")]
    NotFound,

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("compression error: {0}")]
    Compression(String),

    #[error("database is closed")]
    Closed,
}
