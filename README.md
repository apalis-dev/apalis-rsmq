# apalis-rsmq

An [apalis](https://github.com/apalis-dev/apalis) backend for **RSMQ**. Workers consume jobs stored in Redis while keeping the standard `apalis` worker, retry, timeout, and middleware abstractions.

Because RSMQ is a lightweight Redis-based protocol with clients in many languages, `apalis-rsmq` fits systems where jobs are produced or consumed by existing RSMQ-compatible services.

## Features

- **Acknowledgements**: successfully processed messages are acknowledged and removed from the queue.
- **Visibility timeout**: messages stay hidden while being processed and reappear if not acknowledged in time.
- **Delayed messages**: uses RSMQ's delayed-delivery semantics.
- **Message metadata**: supports metadata attached to queued messages.
- **Workflows**: supports sequential workflows.
- **apalis middleware**: retries, timeouts, concurrency limits, tracing, and failure handling.
- **Configurable polling**: tune queue polling and batch size per worker.
- **Atomic operations**: implemented with Redis commands and Lua scripts.
- **Cross-language interoperability**: producers written in any language can enqueue jobs for workers.

## Installation

```toml
[dependencies]
apalis-rsmq = "0.1.0-rc.1"
apalis = "1.0.0-rc.10"
serde = { version = "1.0", features = ["derive"] }
futures = "0.3"
tracing = "0.1"
```

## Usage

### Getting started

Create a `RedisMq` backend from an async Redis connection and pass it to a worker:

```rust
use apalis::prelude::*;
use apalis_rsmq::RedisMq;
use std::env;

async fn handle_message(job: u32, wrk: WorkerContext) -> Result<(), BoxDynError> {
    wrk.stop().unwrap();
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = redis::Client::open(env::var("REDIS_URL")?)?;
    let conn = client.get_multiplexed_async_connection().await?;

    let mut backend = RedisMq::new(conn);

    backend.push(42u32).await.unwrap();

    let worker = WorkerBuilder::new("email-worker")
        .backend(backend)
        .build(handle_message);
    worker.run().await?;
    Ok(())
}
```

RSMQ handles message storage; `apalis` handles worker execution and the task lifecycle.

### Configuration

Use `Config` to set the queue and backend behaviour:

```rust,ignore
use apalis_rsmq::{Config, RedisMq};

let config = Config::default()
    .queue("emails")
    .batch_size(10);

let backend = RedisMq::new(conn).with_config(config);
```

Configurable properties include the queue name, visibility timeout, message delay, maximum queue size, and namespace.

### Retries, timeouts, and concurrency

RSMQ provides persistence and visibility; Middleware provides processing policies. For example, to limit concurrency:

```rust,ignore
let worker = WorkerBuilder::new("email-worker")
    .backend(backend)
    .concurrency(10)
    .build(send_email);
```

### Cross-language integration

Producers and consumers don't need to share a language. A non-Rust application can enqueue jobs with any RSMQ-compatible client, and an `apalis-rsmq` worker will consume them from the same queue:

The payload format is up to your application. For Rust-to-Rust workloads, the same `serde` representation on both sides is convenient. For polyglot systems, use a language-neutral format such as JSON and define the message schema explicitly.

### Using the RSMQ facade

`RedisMqFacade` exposes the underlying RSMQ operations without a worker. Use it to provision queues or to build RSMQ-compatible producers in Rust:

```rust,ignore
use apalis_rsmq::RedisMqFacade;

let facade = RedisMqFacade::new();

facade
    .create_queue(&mut conn, "apalis", "emails", None, None, None)
    .await?;
```

It supports creating, deleting, inspecting, and updating queues, as well as sending, receiving, deleting, and managing messages.

## Architecture

| Component       | Responsibility                                    |
| --------------- | ------------------------------------------------- |
| Redis           | Message persistence and queue semantics           |
| `RedisMqFacade` | Low-level RSMQ operations                         |
| `RedisMq`       | Adapts RSMQ messages to the `Backend` interface   |
| Workers         | Task execution, lifecycle                         |
| Middleware      | Observability, concurrency, retries, timeouts etc |

This keeps the queueing layer and the worker runtime independently configurable.

## License

Licensed under either of **MIT** or **Apache-2.0**.
