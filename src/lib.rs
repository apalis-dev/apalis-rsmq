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
    fmt,
    task::{Context, Poll},
};

use apalis_codec::json::JsonCodec;
use apalis_core::{
    backend::{
        Backend, BackendConfig, WireFormatBackend, finalize::Durable, future::BoxSyncFuture,
    },
    features_table,
    task::{Task, task_id::RandomId},
    worker::{context::WorkerContext, ext::ack::AcknowledgeLayer},
};
use futures::FutureExt;
use redis::aio::{ConnectionLike, ConnectionManager};

use crate::sink::RsMqSink;
pub use crate::{config::Config, error::Error};
pub use crate::{facade::RedisMqFacade, types::Message};

mod ack;
mod config;
mod error;
mod facade;
mod helpers;
mod sink;
mod types;

pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;

/// A task backed by an RSMQ message.
pub type RsMqTask<Args = Vec<u8>> = Task<Args>;

/// An `rsmq` backed message queue
#[doc = features_table! {
    setup = r#"{
        use std::env;
        use apalis_rsmq::{RedisMq, Config};

        let client = redis::Client::open(env::var("REDIS_URL").unwrap()).unwrap();
        let conn = client.get_multiplexed_async_connection().await.unwrap();
        let backend = RedisMq::<u32, _>::new(conn);
        backend
    };"#,
    TaskSink => supported("Ability to push new tasks"),
    Serialization => supported("Serialization support for arguments. Accepts any bytes codec", true),
    FetchById => not_implemented("Allow fetching a task by its ID"),
    RegisterWorker => not_supported("Allow registering a worker with the backend"),
    Workflow => supported("Flexible enough to support workflows", true),
    WaitForCompletion => not_implemented("Wait for tasks to complete without blocking"),
    ResumeById => not_implemented("Resume a task by its ID"),
    ResumeAbandoned => not_implemented("Resume abandoned tasks"),
    ListWorkers => not_implemented("List all workers registered with the backend"),
    ListTasks => not_implemented("List all tasks in the backend"),
}]
#[derive(Debug)]
pub struct RedisMq<Args, Conn = ConnectionManager> {
    connection: Conn,
    facade: RedisMqFacade,
    config: Config,
    codec: JsonCodec,
    sink: RsMqSink<Args>,
    state: State,
}

impl<Args, Conn> Clone for RedisMq<Args, Conn>
where
    Conn: Clone,
{
    fn clone(&self) -> Self {
        Self {
            connection: self.connection.clone(),
            facade: self.facade.clone(),
            config: self.config.clone(),
            codec: self.codec.clone(),
            sink: self.sink.clone(),
            state: State::Init,
        }
    }
}

impl<T, Conn: Clone> RedisMq<T, Conn> {
    /// Creates a new RedisMq instance
    pub fn new(conn: Conn) -> RedisMq<T, Conn> {
        RedisMq {
            sink: RsMqSink::new(),
            connection: conn,
            config: Config::default(),
            codec: JsonCodec::default(),
            state: State::Init,
            facade: RedisMqFacade::default(),
        }
    }

    /// Apply a config to a new instance
    pub fn with_config(mut self, config: Config) -> RedisMq<T, Conn> {
        self.config = config;
        self
    }
}

enum State {
    Init,
    CreateQueue(BoxSyncFuture<Result<(), Error>>),
    Running(Option<BoxSyncFuture<Result<Option<Message>, Error>>>),
}

impl fmt::Debug for State {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Init => f.write_str("Init"),
            Self::CreateQueue(_) => f.write_str("CreateQueue(..)"),
            Self::Running(_) => f.write_str("Running"),
        }
    }
}

impl<Args, C> Backend for RedisMq<Args, C>
where
    Args: Send + Sync + 'static + Unpin,
    C: ConnectionLike + Clone + Send + 'static,
{
    type Task = RsMqTask;
    type Error = Error;

    fn poll_ready(
        &mut self,
        cx: &mut Context<'_>,
        _worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        loop {
            match &mut self.state {
                State::Init => {
                    let client = self.facade.clone();
                    let mut conn = self.connection.clone();
                    let config = self.config.clone();
                    self.state = State::CreateQueue(
                        async move {
                            client
                                .create_queue(
                                    &mut conn,
                                    &config.namespace,
                                    &config.queue,
                                    config.hidden,
                                    config.delay,
                                    config.maxsize,
                                )
                                .await
                        }
                        .boxed()
                        .into(),
                    );
                }

                State::CreateQueue(fut) => match fut.poll_unpin(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(_)) | Poll::Ready(Err(Error::QueueExists)) => {
                        self.state = State::Running(None);
                    }
                    Poll::Ready(Err(e)) => {
                        return Poll::Ready(Err(e));
                    }
                },

                State::Running(_) => return Poll::Ready(Ok(())),
            }
        }
    }

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        _worker: &WorkerContext,
    ) -> Poll<Option<Result<Self::Task, Self::Error>>> {
        match &mut self.state {
            State::Running(fut) => {
                if fut.is_none() {
                    let config = self.config.clone();
                    let client = self.facade.clone();
                    let mut conn = self.connection.clone();
                    *fut = Some(
                        async move {
                            client
                                .receive_message(
                                    &mut conn,
                                    &config.namespace,
                                    &config.queue,
                                    config.hidden,
                                )
                                .await
                        }
                        .boxed()
                        .into(),
                    );
                }

                let res = match fut.as_mut().unwrap().poll_unpin(cx) {
                    Poll::Ready(Ok(Some(res))) => res,
                    Poll::Ready(Err(e)) => return Poll::Ready(Some(Err(e))),
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(None)) => {
                        *fut = None;
                        return Poll::Pending; // Here we return pending and park the worker.
                    }
                };

                // Completed: clear it so the next poll starts a fresh fetch.
                *fut = None;
                Poll::Ready(Some(Ok(res.into())))
            }
            _ => unreachable!(),
        }
    }
    fn poll_close(
        &mut self,
        _cx: &mut Context<'_>,
        _worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

impl<Args, C> BackendConfig for RedisMq<Args, C>
where
    C: Clone,
{
    type Args = Args;

    type Id = RandomId;

    type Kind = Durable;

    type Config = Config;

    type Layer = AcknowledgeLayer<Self>;

    fn config(&self) -> &Self::Config {
        &self.config
    }

    fn middleware(&mut self, _: &mut WorkerContext) -> Self::Layer {
        AcknowledgeLayer::new(self.clone())
    }
}

impl<Args, C> WireFormatBackend for RedisMq<Args, C> {
    type Codec = JsonCodec;

    type Compact = Vec<u8>;

    fn codec(&self) -> &Self::Codec {
        &self.codec
    }
}
