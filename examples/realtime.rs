use std::{env, time::Duration};

use apalis_core::{
    backend::{TaskSink, ext::BackendExt},
    error::BoxDynError,
    worker::{builder::WorkerBuilder, context::WorkerContext},
};
use apalis_rsmq::{Config, RedisMq};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct Email {
    to: String,
    text: String,
    subject: String,
}

async fn send_email(job: Email, wrk: WorkerContext) -> Result<(), BoxDynError> {
    tracing::info!("Sending email to: {}", job.to);
    if job.to == "9" {
        wrk.stop().unwrap();
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), BoxDynError> {
    tracing_subscriber::fmt::init();

    let client = redis::Client::open(env::var("REDIS_URL").unwrap())?;

    let conn = client.get_multiplexed_async_connection().await?;

    let mut pubsub = client.get_async_pubsub().await?;

    pubsub.subscribe("rsmq:rt:jobs").await?;

    let messages = pubsub.into_on_message();

    let config = Config::default()
        .queue("jobs")
        .batch_size(100)
        .realtime(true);

    let backend = RedisMq::new(conn)
        .with_config(config)
        .poll_with_stream(messages);
    let mut b = backend.clone();
    tokio::spawn(async move {
        for index in 0..10 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            b.push(Email {
                to: index.to_string(),
                text: "Test background job from apalis".to_owned(),
                subject: "Background email job".to_owned(),
            })
            .await
            .unwrap();
        }
    });

    let worker = WorkerBuilder::new("rango-tango")
        .backend(backend)
        .build(send_email);

    worker.run().await.unwrap();
    Ok(())
}
