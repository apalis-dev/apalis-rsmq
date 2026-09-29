use redis::ParsingError;

/// Errors returned by the RSMQ backend.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An error returned by Redis.
    #[error("Redis: {0}")]
    Redis(#[from] redis::RedisError),

    /// An error while parsing an RSMQ value.
    #[error("Parse: {0}")]
    Parsing(#[from] ParsingError),

    /// An error while encoding or decoding JSON.
    #[error("Json: {0}")]
    Json(#[from] serde_json::Error),

    /// The requested queue does not exist.
    #[error("queue not found")]
    QueueNotFound,

    /// A queue with the requested name already exists.
    #[error("queue already exists")]
    QueueExists,

    /// The message exceeds the maximum allowed size.
    #[error("message too long")]
    MessageTooLong,

    /// The queue name or message ID has an invalid format.
    #[error("invalid queue name or id")]
    InvalidFormat,

    /// A provided value is invalid.
    #[error("invalid value: {0}")]
    InvalidValue(&'static str),

    /// Message metadata could not be encoded or decoded.
    #[error("metadata encoding: {0}")]
    Metadata(String),
}
