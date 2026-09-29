use std::time::Duration;

use apalis_core::task::{
    builder::TaskBuilder, metadata::MetadataStore, status::Status, task_id::TaskId,
};

use crate::RsMqTask;
/// Attributes and statistics for an RSMQ queue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueAttributes {
    /// Visibility timeout for received messages.
    pub vt: Duration,
    /// Default delay for newly sent messages.
    pub delay: Duration,
    /// Maximum number of messages allowed in the queue.
    pub maxsize: i64,
    /// Total number of messages received.
    pub totalrecv: u64,
    /// Total number of messages sent.
    pub totalsent: u64,
    /// Queue creation time as Unix seconds.
    pub created: u64,
    /// Last modification time as Unix seconds.
    pub modified: u64,
    /// Total number of messages in the queue.
    pub msgs: u64,
    /// Number of messages currently hidden.
    pub hiddenmsgs: u64,
}

/// Optional attributes to update on a queue.
#[derive(Debug, Clone, Default)]
pub struct QueueAttributeUpdate {
    /// Updates the visibility timeout.
    pub hidden: Option<Duration>,
    /// Updates the default message delay.
    pub delay: Option<Duration>,
    /// Updates the maximum queue size.
    pub maxsize: Option<i64>,
}

/// A message received from an RSMQ queue.
#[derive(Debug, Clone)]
pub struct Message {
    /// Unique message identifier.
    pub id: String,
    /// Serialized message payload.
    pub message: Vec<u8>,
    /// Message metadata.
    pub metadata: MetadataStore,
    /// Number of times the message has been received.
    pub attempt: u64,
    /// Time the message was received, as Unix seconds.
    pub received_at: u64,
}

impl From<Message> for RsMqTask {
    fn from(val: Message) -> Self {
        let max_attempts = val
            .metadata
            .get("core.max_attempts")
            .and_then(|a| a.parse().ok())
            .unwrap_or(25);

        let status = val
            .metadata
            .get("core.status")
            .and_then(|a| a.parse().ok())
            .unwrap_or(Status::Pending);

        TaskBuilder::new(val.message)
            .task_id(TaskId::from_string(val.id))
            .max_attempts(max_attempts)
            .status(status)
            .attempt(val.attempt as usize)
            .with_metadata(val.metadata)
            .lock_at(Some(val.received_at))
            .build()
    }
}
