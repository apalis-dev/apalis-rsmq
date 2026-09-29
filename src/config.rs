use std::time::Duration;

/// Configuration for RedisMq
#[derive(Clone, Debug)]
pub struct Config {
    /// Maximum number of tasks to process in a batch.
    pub batch_size: usize,

    /// Queue name.
    pub queue: String,

    /// Namespace used for Redis keys.
    pub namespace: String,

    /// Enable realtime notifications.
    pub realtime: bool,

    /// How long a task should remain hidden after being fetched.
    pub hidden: Option<Duration>,

    /// Delay before a task becomes available.
    pub delay: Option<Duration>,

    /// Maximum queue size.
    pub maxsize: Option<i64>,
}

impl Config {
    /// Sets the namespace.
    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = namespace.into();
        self
    }

    /// Sets the queue.
    pub fn queue(mut self, queue: impl Into<String>) -> Self {
        self.queue = queue.into();
        self
    }

    /// Sets the batch size.
    pub fn batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Sets whether realtime notifications are enabled.
    pub fn realtime(mut self, realtime: bool) -> Self {
        self.realtime = realtime;
        self
    }

    /// Sets the task visibility timeout.
    pub fn hidden(mut self, hidden: Option<Duration>) -> Self {
        self.hidden = hidden;
        self
    }

    /// Sets the delay before tasks become available.
    pub fn delay(mut self, delay: Option<Duration>) -> Self {
        self.delay = delay;
        self
    }

    /// Sets the maximum queue size.
    pub fn maxsize(mut self, maxsize: Option<i64>) -> Self {
        self.maxsize = maxsize;
        self
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            batch_size: 10,
            queue: "default_queue".to_string(),
            namespace: "rsmq".to_string(),
            realtime: false,
            hidden: None,
            delay: None,
            maxsize: None,
        }
    }
}
