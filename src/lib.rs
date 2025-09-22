#![doc = include_str!("../README.md")]
#![warn(
    missing_debug_implementations,
    missing_docs,
    rust_2018_idioms,
    unreachable_pub,
    bad_style,
    dead_code,
    improper_ctypes,
    non_shorthand_field_patterns,
    no_mangle_generic_items,
    overflowing_literals,
    path_statements,
    patterns_in_fns_without_body,
    unconditional_recursion,
    unused,
    unused_allocation,
    unused_comparisons,
    unused_parens,
    while_true
)]

use std::{
    fmt::Debug,
    marker::PhantomData,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Instant,
};

use apalis_core::{
    backend::{
        codec::{json::JsonCodec, Codec},
        poll_strategy::{PollContext, PollStrategyExt},
        Backend, TaskStream,
    },
    features_table,
    task::{attempt::Attempt, builder::TaskBuilder, task_id::TaskId, Task},
    worker::{context::WorkerContext, ext::ack::AcknowledgeLayer},
};
use futures::{
    future,
    stream::{self, BoxStream},
    StreamExt,
};
use rsmq_async::{Rsmq, RsmqConnection, RsmqError};
use serde::de::DeserializeOwned;
use tracing::{error, trace, warn};

use crate::sink::RsMqSink;

mod ack;
mod config;
mod context;
mod sink;

pub use crate::config::Config;
pub use crate::context::RedisMqContext;

type RsMqTask<T> = Task<T, RedisMqContext, String>;

pin_project_lite::pin_project! {
    /// Redis-backed message queue
    ///
    #[doc = features_table! {
        setup = {
            use apalis_rsmq::Config;
            use apalis_rsmq::RedisMq;
            use rsmq_async::RsmqConnection;

            let mut conn = rsmq_async::Rsmq::new(Default::default()).await.unwrap();
            let _ = conn.create_queue("test", None, None, None).await;
            let mut config = Config::default();
            config.set_namespace("test".to_owned());
            let mut mq = RedisMq::new(conn, config);
            mq
        };,
        TaskSink => supported("Ability to push new tasks"),
        Serialization => supported("Serialization support for arguments. Accepts any bytes codec", false),
        FetchById => not_implemented("Allow fetching a task by its ID"),
        RegisterWorker => not_supported("Allow registering a worker with the backend"),
        PipeExt => supported("Allow other backends to pipe to this backend", false),
        MakeShared => supported("Share the same JSON storage across multiple workers", false),
        Workflow => supported("Flexible enough to support workflows", false),
        WaitForCompletion => supported("Wait for tasks to complete without blocking", false),
        ResumeById => not_implemented("Resume a task by its ID"),
        ResumeAbandoned => not_implemented("Resume abandoned tasks"),
        ListWorkers => not_supported("List all workers registered with the backend"),
        ListTasks => not_implemented("List all tasks in the backend"),
    }]
    #[derive(Debug)]
    pub struct RedisMq<T, C = JsonCodec<Vec<u8>>> {
        conn: Rsmq,
        config: Config,
        msg_type: PhantomData<T>,
        codec: PhantomData<C>,
        #[pin]
        sink: RsMqSink<T, C>,
    }
}

impl<T> RedisMq<T, JsonCodec<Vec<u8>>> {
    /// Creates a new RedisMq instance
    pub fn new(conn: Rsmq, config: Config) -> RedisMq<T, JsonCodec<Vec<u8>>> {
        RedisMq {
            sink: RsMqSink::new(conn.clone(), config.clone()),
            conn,
            config,
            msg_type: PhantomData,
            codec: PhantomData,
        }
    }
}

impl<T, C> RedisMq<T, C> {
    /// Gets the configuration
    pub fn config(&self) -> &Config {
        &self.config
    }
}

// Implement Clone manually
impl<T, C> Clone for RedisMq<T, C> {
    fn clone(&self) -> Self {
        RedisMq {
            conn: self.conn.clone(),
            msg_type: PhantomData,
            config: self.config.clone(),
            codec: PhantomData,
            sink: RsMqSink::new(self.conn.clone(), self.config.clone()),
        }
    }
}

impl<Args, C> Backend<Args> for RedisMq<Args, C>
where
    Args: Send + DeserializeOwned + 'static,
    C: Codec<PrimitiveMessage<Args>, Compact = Vec<u8>>,
    C::Error: std::error::Error + Send,
{
    type Stream = TaskStream<RsMqTask<Args>, RsmqError>;
    type Layer = AcknowledgeLayer<Self>;
    type Codec = C;
    type Context = RedisMqContext;
    type Error = RsmqError;
    type Beat = BoxStream<'static, Result<(), RsmqError>>;
    type IdType = String;

    fn heartbeat(&self, _: &WorkerContext) -> Self::Beat {
        stream::once(future::ready(Ok(()))).boxed()
    }

    fn middleware(&self) -> Self::Layer {
        AcknowledgeLayer::new(self.clone())
    }

    fn poll(self, worker: &WorkerContext) -> Self::Stream {
        let poll_strategy = self.config.poll_strategy().clone();
        let prev_count = Arc::new(AtomicUsize::new(0));
        let ctx = PollContext::new(worker.clone(), prev_count.clone());
        let throttle = poll_strategy.build_stream(&ctx);

        let stream = futures::stream::unfold(throttle, move |mut throttle| {
            let mut conn = self.conn.clone();
            let namespace = self.config.namespace().to_string();
            let prev_count = prev_count.clone();
            async move {
                let instant = Instant::now();
                throttle.next().await;
                trace!("Polling new messages after {:?}", instant.elapsed());
                match conn.receive_message(&namespace, None).await {
                    Ok(Some(r)) => match C::decode(&r.message) {
                        Ok(msg) => {
                            let task = TaskBuilder::new(msg.task)
                                .with_ctx(msg.context)
                                .with_task_id(TaskId::new(r.id))
                                .with_attempt(Attempt::new_with_value(r.rc as usize))
                                .build();
                            prev_count.store(1, Ordering::SeqCst);
                            Some((Ok(Some(task)), throttle))
                        }
                        Err(e) => {
                            error!("Failed to decode message: {:?}", e);
                            Some((Err(RsmqError::InvalidFormat(e.to_string())), throttle))
                        }
                    },
                    Ok(None) => {
                        prev_count.store(0, Ordering::SeqCst);
                        Some((Ok(None), throttle))
                    }
                    Err(e) => {
                        error!("Error receiving message: {:?}", e);
                        Some((Err(e), throttle))
                    }
                }
            }
        });

        stream.boxed()
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct PrimitiveMessage<T> {
    task: T,
    context: RedisMqContext,
}
