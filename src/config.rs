//! Configuration for RedisMq
use std::time::Duration;

use apalis_core::backend::poll_strategy::{
    BackoffConfig, IntervalStrategy, MultiStrategy, StrategyBuilder,
};

/// Configuration for RedisMq
#[derive(Clone, Debug)]
pub struct Config {
    poll_strategy: MultiStrategy,
    buffer_size: usize,
    namespace: String,
}

impl Config {
    /// Creates a new configuration
    pub fn new(poll_strategy: MultiStrategy, buffer_size: usize, namespace: String) -> Self {
        Self {
            poll_strategy,
            buffer_size,
            namespace,
        }
    }

    /// Gets the namespace
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Sets the namespace
    pub fn set_namespace(&mut self, namespace: String) {
        self.namespace = namespace;
    }

    /// Gets the polling strategy
    pub fn poll_strategy(&self) -> &MultiStrategy {
        &self.poll_strategy
    }

    /// Sets the polling strategy
    pub fn set_poll_strategy(&mut self, strategy: MultiStrategy) {
        self.poll_strategy = strategy;
    }

    /// Gets the buffer size
    pub fn buffer_size(&self) -> usize {
        self.buffer_size
    }

    /// Sets the buffer size
    pub fn set_buffer_size(&mut self, size: usize) {
        self.buffer_size = size;
    }
}

impl Default for Config {
    fn default() -> Self {
        let config = BackoffConfig::default().with_jitter(0.9);
        let interval = IntervalStrategy::new(Duration::from_millis(50)).with_backoff(config);
        let poll_strategy = StrategyBuilder::new().apply(interval).build();
        Self {
            poll_strategy,
            buffer_size: 100,
            namespace: "default_queue".to_string(),
        }
    }
}
