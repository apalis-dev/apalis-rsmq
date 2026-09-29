use std::fmt::Debug;

use crate::{RedisMq, error::Error};
use apalis_core::{
    error::BoxDynError,
    task::{ExecutionContext, status::Status},
    worker::ext::ack::Acknowledge,
};
use futures::{
    FutureExt,
    future::{self, BoxFuture},
};
use redis::aio::ConnectionLike;

impl<T, Conn, Res> Acknowledge<Res> for RedisMq<T, Conn>
where
    T: Send + 'static,
    Res: Debug + Send + Sync,
    Conn: Send + Clone + ConnectionLike + 'static,
{
    type Error = Error;

    type Future = BoxFuture<'static, Result<(), Self::Error>>;

    fn ack(&mut self, _res: &Result<Res, BoxDynError>, ctx: &ExecutionContext) -> Self::Future {
        match ctx.status() {
            Status::Done | Status::Killed => {
                let task_id = ctx.task_id().as_ref().unwrap().to_string();
                let queue = self.config.queue.to_owned();
                let namespace = self.config.namespace.to_owned();
                let client = self.facade.clone();
                let mut conn = self.connection.clone();

                let fut = async move {
                    client
                        .delete_message(&mut conn, &namespace, &queue, &task_id)
                        .map(move |r| match r {
                            Err(e) => Err(e),
                            Ok(true) => Ok(()),
                            Ok(false) => Err(Error::InvalidValue("TaskIdInvalid")),
                        })
                        .await?;
                    Ok(())
                };
                return fut.boxed();
            }
            Status::Failed => {
                // TODO: update the attempts in metadata? t
            }
            _ => unreachable!(),
        }
        future::ready(Ok(())).boxed()
    }
}
