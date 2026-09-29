use std::{env, time::Duration};

use apalis::layers::WorkerBuilderExt;
use apalis_core::{
    backend::{TaskSink, ext::BackendExt},
    error::BoxDynError,
    worker::{builder::WorkerBuilder, context::WorkerContext},
};
use apalis_rsmq::{Config, RedisMq};
use apalis_workflow::SteppedFlow;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct Email {
    to: String,
    text: String,
    subject: String,
}

async fn send_email(job: Email) -> Result<u32, BoxDynError> {
    tracing::info!("Sending email to: {}", job.to);
    Ok(42)
}

#[tokio::main]
async fn main() -> Result<(), BoxDynError> {
    tracing_subscriber::fmt::init();

    let client = redis::Client::open(env::var("REDIS_URL").unwrap())?;

    let conn = client.get_connection_manager().await?;

    let config = Config::default()
        .queue("email-workflow")
        .batch_size(100)
        .realtime(true);

    let mut backend = RedisMq::new(conn)
        .with_config(config)
        .poll_with_interval(Duration::from_secs(1));
    backend
        .push(Email {
            to: "test@email.com".to_string(),
            text: "Test background job from apalis".to_owned(),
            subject: "Background email job".to_owned(),
        })
        .await
        .unwrap();

    let workflow = SteppedFlow::new("email-workflow")
        .and_then(send_email)
        .delay_for(Duration::from_secs(2))
        .and_then(|id, worker: WorkerContext| async move {
            assert_eq!(id, 42);
            tracing::info!("Done processing workflow");
            worker.stop()
        });

    let worker = WorkerBuilder::new("rango-tango")
        .backend(backend)
        .enable_tracing()
        .build(workflow);

    worker.run().await.unwrap();
    Ok(())
}
