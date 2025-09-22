use apalis_core::{
    backend::TaskSink,
    error::BoxDynError,
    worker::{builder::WorkerBuilder, context::WorkerContext},
};
use apalis_rsmq::{Config, RedisMq};
use rsmq_async::RsmqConnection;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct Email {
    to: String,
    text: String,
    subject: String,
}

async fn produce_jobs(mq: &mut RedisMq<Email>) -> Result<(), rsmq_async::RsmqError> {
    for index in 0..100 {
        mq.push(Email {
            to: index.to_string(),
            text: "Test background job from apalis".to_owned(),
            subject: "Background email job".to_owned(),
        })
        .await?;
        println!("Produced job: {}", index);
    }
    Ok(())
}

async fn send_email(job: Email, wrk: WorkerContext) -> Result<(), BoxDynError> {
    tracing::info!("Sending email to: {}", job.to);
    if job.to == "99" {
        tokio::time::sleep(std::time::Duration::from_secs(6)).await;
        wrk.stop().unwrap();
    }

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
    produce_jobs(&mut mq).await?;

    let worker = WorkerBuilder::new("rango-tango")
        .backend(mq)
        .build(send_email);

    worker.run().await.unwrap();
    Ok(())
}
