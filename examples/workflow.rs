use std::time::Duration;

use apalis_core::{
    error::BoxDynError,
    worker::{builder::WorkerBuilder, context::WorkerContext},
};
use apalis_rsmq::{Config, RedisMq};
use apalis_workflow::{TaskFlowSink, WorkFlow};
use rsmq_async::RsmqConnection;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct Email {
    to: String,
    text: String,
    subject: String,
}

async fn send_email(job: Email, wrk: WorkerContext) -> Result<(), BoxDynError> {
    tracing::info!("Sending email to: {}", job.to);

    tokio::time::sleep(std::time::Duration::from_secs(6)).await;
    wrk.stop().unwrap();

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), rsmq_async::RsmqError> {
    tracing_subscriber::fmt::init();

    let mut conn = rsmq_async::Rsmq::new(Default::default()).await?;
    let _ = conn.create_queue("email", None, None, None).await;
    let mut config = Config::default();
    config.set_namespace("email".to_owned());
    let mut mq = RedisMq::new(conn, config);
    mq.push_start(Email {
        to: "0".to_string(),
        text: "Test background job from apalis".to_owned(),
        subject: "Background email job".to_owned(),
    })
    .await
    .unwrap();

    let workflow = WorkFlow::new("email-workflow")
        .then(send_email)
        .delay_for(Duration::from_secs(2))
        .then(|_| async {
            println!("Done processing workflow");
        });

    let worker = WorkerBuilder::new("rango-tango")
        .backend(mq)
        .build(workflow);

    worker.run().await.unwrap();
    Ok(())
}
