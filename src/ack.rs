use std::fmt::Debug;

use crate::{context::RedisMqContext, RedisMq};
use apalis_core::{error::BoxDynError, task::Parts, worker::ext::ack::Acknowledge};
use futures::{
    future::{self, BoxFuture},
    FutureExt,
};
use rsmq_async::{RsmqConnection, RsmqError};

impl<T, C, Res> Acknowledge<Res, RedisMqContext, String> for RedisMq<T, C>
where
    T: Send,
    Res: Debug + Send + Sync,
    C: Send,
{
    type Error = RsmqError;

    type Future = BoxFuture<'static, Result<(), Self::Error>>;

    fn ack(
        &mut self,
        res: &Result<Res, BoxDynError>,
        parts: &Parts<RedisMqContext, String>,
    ) -> Self::Future {
        if res.is_ok() || parts.attempt.current() >= parts.ctx.max_attempts() {
            let task_id = parts.task_id.as_ref().unwrap().inner().to_owned();
            let namespace = self.config.namespace().to_owned();
            let mut conn = self.conn.clone();

            let fut = async move {
                conn.delete_message(&namespace, &task_id)
                    .map(move |r| match r {
                        Err(e) => Err(e),
                        Ok(true) => Ok(()),
                        Ok(false) => {
                            Err(RsmqError::MissingParameter("TaskId not found".to_owned()))
                        }
                    })
                    .await?;
                Ok(())
            };
            return fut.boxed();
        }
        future::ready(Ok(())).boxed()
    }
}
