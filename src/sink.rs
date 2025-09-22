use std::{
    collections::VecDeque,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use apalis_core::backend::codec::Codec;
use chrono::{TimeZone, Utc};
use futures::{FutureExt, Sink};
use rsmq_async::{Rsmq, RsmqConnection, RsmqError};

use crate::{PrimitiveMessage, RedisMq, RsMqTask, config::Config};

pin_project_lite::pin_project! {
    pub(super) struct RsMqSink<T, C> {
        conn: Rsmq,
        config: Config,
        items: VecDeque<RsMqTask<T>>,
        pending_sends: VecDeque<PendingSend>,
        _codec: std::marker::PhantomData<C>,
    }
}
impl<T, C> Clone for RsMqSink<T, C> {
    fn clone(&self) -> Self {
        Self {
            conn: self.conn.clone(),
            config: self.config.clone(),
            items: VecDeque::new(),
            pending_sends: VecDeque::new(),
            _codec: std::marker::PhantomData,
        }
    }
}

impl<T, C> std::fmt::Debug for RsMqSink<T, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RsMqSink")
            .field("config", &self.config)
            .field("items_len", &self.items.len())
            .field("pending_sends_len", &self.pending_sends.len())
            .finish()
    }
}

impl<T, C> RsMqSink<T, C> {
    pub(crate) fn new(conn: Rsmq, config: Config) -> Self {
        Self {
            conn,
            config,
            items: VecDeque::new(),
            pending_sends: VecDeque::new(),
            _codec: std::marker::PhantomData,
        }
    }
}

struct PendingSend {
    future: Pin<Box<dyn Future<Output = Result<String, RsmqError>> + Send + 'static>>,
}
// SAFETY: PendingSend contains a Pin<Box<dyn Future + Send>>, which is Send but not Sync.
unsafe impl Sync for PendingSend {}

impl<T, C> Sink<RsMqTask<T>> for RedisMq<T, C>
where
    C::Error: std::error::Error + Send,
    C: Codec<PrimitiveMessage<T>, Compact = Vec<u8>>,
{
    type Error = RsmqError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // First, try to flush any pending sends
        let this = self.get_mut();

        // Poll pending sends
        while let Some(pending) = this.sink.pending_sends.front_mut() {
            match pending.future.as_mut().poll(cx) {
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

    fn start_send(self: Pin<&mut Self>, item: RsMqTask<T>) -> Result<(), Self::Error> {
        let this = self.project().sink;
        let items = this.get_mut();
        items.items.push_back(item);
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        let namespace = this.config.namespace();

        // First, convert any queued items to pending sends
        while let Some(item) = this.sink.items.pop_front() {
            let delay = delay_until(item.parts.run_at);
            let bytes = match C::encode(&PrimitiveMessage {
                task: item.args,
                context: item.parts.ctx,
            }) {
                Ok(bytes) => bytes,
                Err(e) => return Poll::Ready(Err(RsmqError::InvalidFormat(e.to_string()))),
            };
            let mut conn = this.conn.clone();
            let namespace = namespace.to_string();
            // Create the future but don't poll it yet
            let future =
                async move { conn.send_message(&namespace, bytes, Some(delay)).await }.boxed();
            this.sink.pending_sends.push_back(PendingSend { future });
        }

        // Now poll all pending sends
        while let Some(pending) = this.sink.pending_sends.front_mut() {
            match pending.future.as_mut().poll(cx) {
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
    let target_time = Utc.timestamp_opt(timestamp as i64, 0).unwrap();
    let now = Utc::now();

    if target_time <= now {
        Duration::from_secs(0)
    } else {
        let diff = target_time - now;
        Duration::from_secs(diff.num_seconds() as u64)
    }
}
