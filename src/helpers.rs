use std::collections::HashMap;

use rand::RngExt;

use crate::{Error, Result};

pub(crate) fn validate_queue_name(q: &str) -> Result<()> {
    if q.is_empty()
        || q.len() > 160
        || !q
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return Err(Error::InvalidFormat);
    }
    Ok(())
}

pub(crate) fn valid_id(id: &str) -> bool {
    id.len() == 32 && id.chars().all(|c| c.is_ascii_alphanumeric())
}

/// RSMQ ids: base36(microsecond timestamp) + 22 random base36 chars = 32 chars.
pub(crate) fn make_id(secs: u64, micros: u64) -> String {
    const ALPHABET: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let ts = secs * 1_000_000 + micros;
    let mut prefix = to_base36(ts);
    let mut rng = rand::rng();
    while prefix.len() < 32 {
        prefix.push(ALPHABET[rng.random_range(0..36)] as char);
    }
    prefix
}

pub(crate) fn to_base36(mut n: u64) -> String {
    const ALPHABET: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    if n == 0 {
        return "0".into();
    }
    let mut out = Vec::new();
    while n > 0 {
        out.push(ALPHABET[(n % 36) as usize]);
        n /= 36;
    }
    out.reverse();
    String::from_utf8(out).unwrap()
}

/// Metadata is stored as a JSON object in the `<id>:meta` hash field.
/// Empty map => field is omitted entirely (byte-identical to stock RSMQ).
pub(crate) fn encode_metadata(m: &HashMap<String, String>) -> Result<String> {
    if m.is_empty() {
        return Ok(String::new());
    }
    serde_json::to_string(m).map_err(|e| Error::Metadata(e.to_string()))
}

pub(crate) fn map_script_err<T>(r: Result<T, redis::RedisError>) -> Result<T> {
    r.map_err(|e| match e.detail() {
        Some("QueueNotFound") => Error::QueueNotFound,
        Some("MessageTooLong") => Error::MessageTooLong,
        _ => Error::Redis(e),
    })
}
