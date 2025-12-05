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
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

use apalis_core::{
    backend::{
        Backend, TaskStream,
        codec::{Codec, json::JsonCodec},
        poll_strategy::{PollContext, PollStrategyExt},
    },
    features_table,
    task::{Task, attempt::Attempt, builder::TaskBuilder, task_id::TaskId},
    worker::{context::WorkerContext, ext::ack::AcknowledgeLayer},
};
use futures::{
    StreamExt, future,
    stream::{self, BoxStream},
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
        setup = r#"{
            use apalis_rsmq::Config;
            use apalis_rsmq::RedisMq;
            use rsmq_async::RsmqConnection;

            let mut conn = rsmq_async::Rsmq::new(Default::default()).await.unwrap();
            let _ = conn.create_queue("test", None, None, None).await;
            let mut config = Config::default();
            config.set_namespace("test".to_owned());
            let mut mq = RedisMq::new(conn, config);
            mq
        }"#,
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

impl<Args, C, Err> Backend for RedisMq<Args, C>
where
    Args: Send + DeserializeOwned + 'static,
    C: Codec<PrimitiveMessage<Args>, Compact = Vec<u8>, Error = Err>
        + Codec<Args, Compact = Vec<u8>, Error = Err>,
    Err: std::error::Error + Send,
{
    type Args = Args;
    type Stream = TaskStream<RsMqTask<Args>, RsmqError>;
    type Layer = AcknowledgeLayer<Self>;
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
        let poll_control = poll_strategy.build_stream(&ctx);

        let stream = futures::stream::unfold(poll_control, move |mut poll_control| {
            let mut conn = self.conn.clone();
            let namespace = self.config.namespace().to_string();
            let prev_count = prev_count.clone();
            async move {
                let instant = Instant::now();
                poll_control.next().await;
                trace!("Polling new messages after {:?}", instant.elapsed());
                match conn.receive_message(&namespace, None).await {
                    Ok(Some(r)) => match C::decode(&r.message) {
                        Ok(PrimitiveMessage { task, context }) => {
                            let task = TaskBuilder::new(task)
                                .with_ctx(context)
                                .with_task_id(TaskId::new(r.id))
                                .with_attempt(Attempt::new_with_value(r.rc as usize))
                                .build();
                            prev_count.store(1, Ordering::SeqCst);
                            Some((Ok(Some(task)), poll_control))
                        }
                        Err(e) => {
                            error!("Failed to decode message: {:?}", e);
                            Some((Err(RsmqError::InvalidFormat(e.to_string())), poll_control))
                        }
                    },
                    Ok(None) => {
                        prev_count.store(0, Ordering::SeqCst);
                        Some((Ok(None), poll_control))
                    }
                    Err(e) => {
                        error!("Error receiving message: {:?}", e);
                        Some((Err(e), poll_control))
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

async fn receive_message<T, C, Err>(
    conn: &mut Rsmq,
    namespace: &str,
) -> Result<Option<PrimitiveMessage<T>>, RsmqError>
where
    T: DeserializeOwned,
    C: Codec<PrimitiveMessage<T>, Compact = Vec<u8>, Error = Err>,
    Err: std::error::Error + Send,
{
    // let timestamp = Utc::now().timestamp();
    // let script = Script::new(include_str!("../lua/ack_job.lua"));
    // let mut conn = self.conn.clone();

    // async move {
    //     let mut script = script.key(inflight_set);
    //     let _ = script
    //         .key(done_jobs_set)
    //         .key(dead_jobs_set)
    //         .key(job_meta_hash)
    //         .arg(task_id)
    //         .arg(timestamp)
    //         .arg(result_data)
    //         .arg(status)
    //         .arg(attempt)
    //         .invoke_async::<u32>(&mut conn)
    //         .boxed()
    //         .await?;
    //     Ok(())
    // }
    // .boxed()

    todo!("Implement receive_message function to fetch messages from Redis")
}
