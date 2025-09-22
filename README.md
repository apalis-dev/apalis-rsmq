# apalis-rsmq: A redis-backed message queue build with rust and apalis

**apalis-rsmq** is a message queue implementation that integrates with [`apalis`] to provide a
Redis-based backend for message processing. It uses [`rsmq_async`] for Redis Simple Message Queue (RSMQ) interactions.

## 🚀 Features

- **Message Enqueue & Dequeue**: Supports adding and retrieving messages from Redis queues.
- **Acknowledgments**: Messages can be acknowledged and removed from the queue once processed successfully.
- **Configurable Polling**: Adjustable polling intervals.
- **Automatic Message Processing**: Works with [`Backend`] to process messages asynchronously.

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

## License

Licensed under **MIT** or **Apache-2.0**.
