# apalis-rsmq

[![CI](https://github.com/apalis-dev/apalis-rsmq/workflows/CI/badge.svg)](https://github.com/apalis-dev/apalis-rsmq/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/apalis-dev/apalis-rsmq/branch/main/graph/badge.svg)](https://codecov.io/gh/apalis-dev/apalis-rsmq)
[![Crates.io](https://img.shields.io/crates/v/apalis-rsmq.svg)](https://crates.io/crates/apalis-rsmq)
[![Documentation](https://docs.rs/apalis-rsmq/badge.svg)](https://docs.rs/apalis-rsmq)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![MSRV](https://img.shields.io/badge/MSRV-1.75.0-blue.svg)](https://blog.rust-lang.org/2023/12/28/Rust-1.75.0.html)

A redis-backed message queue built with rust, apalis and rsmq-async

**apalis-rsmq** is a message queue implementation that integrates with [`apalis`] to provide a
Redis-based backend for message processing. It uses [`rsmq_async`] for Redis Simple Message Queue (RSMQ) interactions.

## 🚀 Features

- **High Performance**: Built with Rust for maximum performance and safety
- **Message Enqueue & Dequeue**: Supports adding and retrieving messages from Redis queues
- **Acknowledgments**: Messages can be acknowledged and removed from the queue once processed successfully
- **Configurable Polling**: Adjustable polling intervals
- **Automatic Message Processing**: Works with [`Backend`] to process messages asynchronously
- **Type Safety**: Fully typed message handling with Serde
- **Observability**: Built-in tracing and monitoring capabilities

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
apalis-rsmq = "0.1.0-alpha.1"
apalis = "1.0.0-alpha.2"
serde = { version = "1.0", features = ["derive"] }
futures = "0.3"
tracing = "0.1"
```

## Usage

### **Creating a Message Queue**

```rust
use apalis_rsmq::{RedisMq, Config};
use rsmq_async::Rsmq;
use std::time::Duration;
use rsmq_async::RsmqConnection;

#[tokio::main]
async fn main() {
    let mut conn = Rsmq::new(Default::default()).await.unwrap();
    let _ = conn.create_queue("email", None, None, None).await;
    let mut config = Config::default();
    config.set_namespace("email".to_owned());
    let mq: RedisMq<String> = RedisMq::new(conn, config);
}
```

### **Enqueuing Messages**

```rust
use apalis_rsmq::RedisMq;
use apalis_core::backend::TaskSink;

async fn enqueue_message(mq: &mut RedisMq<String>) {
    mq.push("Hello, Redis!".to_string()).await.unwrap();
}
```

### **Processing Messages Automatically**

```rust
use apalis::prelude::*;
use apalis_rsmq::RedisMq;
async fn task(message: String) {
    // Do something with message
}

async fn start_worker(mq: RedisMq<String>) {
    let worker = WorkerBuilder::new("string-worker").backend(mq).build(task);
    worker.run().await.unwrap();
}
```

## Development

### Prerequisites

- Rust 1.75.0 or later
- Redis server for testing

### Building

```bash
cargo build
```

### Testing

Start a Redis server, then run:

```bash
export REDIS_URL=redis://localhost:6379
cargo test
```

### Formatting and Linting  

```bash
cargo fmt
cargo clippy
```

## CI/CD

This project uses comprehensive GitHub Actions workflows:

- **CI Pipeline**: Automated testing, linting, security audits, and code coverage
- **Release Automation**: Semantic versioning and automated publishing to crates.io
- **Dependency Management**: Automated dependency updates and security audits
- **Documentation**: Automated documentation building and deployment
- **Benchmarking**: Performance regression detection

## Contributing

We welcome contributions! Please follow these steps:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for your changes
5. Ensure all tests pass and code is formatted
6. Submit a pull request

## License

Licensed under **MIT** or **Apache-2.0**.
