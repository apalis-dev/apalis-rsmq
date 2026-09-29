use std::{
    collections::VecDeque,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use apalis_core::backend::future::BoxSyncFuture;
use futures::{FutureExt, Sink};
use redis::aio::ConnectionLike;

use crate::{RedisMq, RsMqTask, State, error::Error};

pub(super) struct RsMqSink<T> {
    items: VecDeque<RsMqTask<Vec<u8>>>,
    pending_sends: VecDeque<BoxSyncFuture<Result<(), Error>>>,
    _args: std::marker::PhantomData<T>,
}

impl<T> Clone for RsMqSink<T> {
    fn clone(&self) -> Self {
        Self {
            items: VecDeque::new(),
            pending_sends: VecDeque::new(),
            _args: std::marker::PhantomData,
        }
    }
}

impl<T> std::fmt::Debug for RsMqSink<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RsMqSink")
            .field("items_len", &self.items.len())
            .field("pending_sends_len", &self.pending_sends.len())
            .finish()
    }
}

impl<T> RsMqSink<T> {
    pub(crate) fn new() -> Self {
        Self {
            items: VecDeque::new(),
            pending_sends: VecDeque::new(),
            _args: std::marker::PhantomData,
        }
    }
}

impl<T, Conn> Sink<RsMqTask<Vec<u8>>> for RedisMq<T, Conn>
where
    Conn: Unpin + Clone + ConnectionLike + Send + 'static,
    T: Unpin + Send + 'static,
{
    type Error = Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // First, try to flush any pending sends
        let this = self.get_mut();

        loop {
            match &mut this.state {
                State::Init => {
                    let client = this.facade.clone();
                    let mut conn = this.connection.clone();
                    let config = this.config.clone();
                    this.state = State::CreateQueue(
                        async move {
                            let res = client
                                .create_queue(
                                    &mut conn,
                                    &config.namespace,
                                    &config.queue,
                                    config.hidden,
                                    config.delay,
                                    config.maxsize,
                                )
                                .await;

                            if let Err(Error::QueueExists) = res {
                                return Ok(());
                            }
                            res
                        }
                        .boxed()
                        .into(),
                    );
                }

                State::CreateQueue(fut) => match fut.poll_unpin(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(e)) => {
                        return Poll::Ready(Err(e));
                    }
                    Poll::Ready(Ok(_)) => {
                        this.state = State::Running(None);
                    }
                },

                State::Running(_) => break,
            }
        }

        // Poll pending sends
        while let Some(pending) = this.sink.pending_sends.front_mut() {
            match pending.poll_unpin(cx) {
                Poll::Ready(Ok(_)) => {
                    this.sink.pending_sends.pop_front();
                }
                Poll::Ready(Err(e)) => {
                    this.sink.pending_sends.pop_front();
                    return Poll::Ready(Err(e));
                }
                Poll::Pending => {
                    return Poll::Pending;
                }
            }
        }

        Poll::Ready(Ok(()))
    }

    fn start_send(mut self: Pin<&mut Self>, item: RsMqTask<Vec<u8>>) -> Result<(), Self::Error> {
        self.as_mut().sink.items.push_back(item);
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();

        // First, convert any queued items to pending sends
        while let Some(item) = this.sink.items.pop_front() {
            let delay = item.run_at().map(delay_until);

            let mut headers = item.metadata().clone().into_inner();

            headers.insert(
                "core.max_attempts".to_owned(),
                item.max_attempts().unwrap_or(25).to_string(),
            );

            headers.insert("core.status".to_owned(), item.status().to_string());

            let client = this.facade.clone();
            let message = item.args;
            let config = this.config.clone();
            let mut conn = this.connection.clone();
            // Create the future but don't poll it yet
            let future = async move {
                client
                    .send_message(
                        &mut conn,
                        &config.namespace,
                        &config.queue,
                        message,
                        headers,
                        config.realtime,
                        delay,
                    )
                    .await?;
                Ok(())
            }
            .boxed()
            .into();
            this.sink.pending_sends.push_back(future);
        }

        // Now poll all pending sends
        while let Some(pending) = this.sink.pending_sends.front_mut() {
            match pending.poll_unpin(cx) {
                Poll::Ready(Ok(_)) => {
                    this.sink.pending_sends.pop_front();
                }
                Poll::Ready(Err(e)) => {
                    this.sink.pending_sends.pop_front();
                    return Poll::Ready(Err(e));
                }
                Poll::Pending => {
                    return Poll::Pending;
                }
            }
        }

        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.poll_flush(cx)
    }
}

fn delay_until(timestamp: u64) -> Duration {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();

    let target = Duration::from_secs(timestamp);

    target.saturating_sub(now)
}
