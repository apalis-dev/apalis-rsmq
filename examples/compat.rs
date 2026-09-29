use std::env;

use apalis_core::{
    error::BoxDynError,
    worker::{builder::WorkerBuilder, context::WorkerContext},
};
use apalis_rsmq::{Config, RedisMq};
use rsmq_async::{Rsmq, RsmqConnection};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct Email {
    to: String,
    text: String,
    subject: String,
}

async fn send_email(job: Email, wrk: WorkerContext) -> Result<(), BoxDynError> {
    tracing::info!("Sending email to: {}", job.to);
    wrk.stop().unwrap();
    Ok(())
}

const QUEUE: &str = "emails-compat";

#[tokio::main]
async fn main() -> Result<(), BoxDynError> {
    tracing_subscriber::fmt::init();

    let client = redis::Client::open(env::var("REDIS_URL").unwrap())?;

    let conn = client.get_multiplexed_async_connection().await?;

    let mut rsmq = Rsmq::new(Default::default()).await?;

    let email = Email {
        to: "test@email.com".to_string(),
        text: "Test background job from apalis".to_owned(),
        subject: "Background email job".to_owned(),
    };

    let _ = rsmq.create_queue(QUEUE, None, None, None).await;
    rsmq.send_message(QUEUE, serde_json::to_vec(&email).unwrap(), None)
        .await?;

    let config = Config::default().queue(QUEUE);
    let backend = RedisMq::new(conn).with_config(config);

    let worker = WorkerBuilder::new("rango-tango")
        .backend(backend)
        .build(send_email);

    worker.run().await.unwrap();
    Ok(())
}
