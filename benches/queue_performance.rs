use apalis_core::backend::TaskSink;
use apalis_rsmq::{Config, RedisMq};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rsmq_async::{Rsmq, RsmqConnection};
use tokio::runtime::Runtime;

fn benchmark_queue_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    // Skip benchmarks if Redis is not available
    let _redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());

    // Test if Redis is available
    if rt
        .block_on(async {
            let conn = Rsmq::new(Default::default()).await?;
            Ok::<_, Box<dyn std::error::Error>>(conn)
        })
        .is_err()
    {
        eprintln!("Skipping benchmarks: Redis not available");
        return;
    }

    c.bench_function("push_message", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut conn = Rsmq::new(Default::default()).await.unwrap();
                let _ = conn.create_queue("bench_queue", None, None, None).await;

                let mut config = Config::default();
                config.set_namespace("bench_queue".to_owned());
                let mut mq: RedisMq<String> = RedisMq::new(conn, config);

                mq.push(black_box("benchmark message".to_string()))
                    .await
                    .unwrap();
            })
        });
    });

    c.bench_function("push_multiple_messages", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut conn = Rsmq::new(Default::default()).await.unwrap();
                let _ = conn
                    .create_queue("bench_queue_multi", None, None, None)
                    .await;

                let mut config = Config::default();
                config.set_namespace("bench_queue_multi".to_owned());
                let mut mq: RedisMq<String> = RedisMq::new(conn, config);

                for i in 0..10 {
                    mq.push(black_box(format!("benchmark message {}", i)))
                        .await
                        .unwrap();
                }
            })
        });
    });
}

criterion_group!(benches, benchmark_queue_operations);
criterion_main!(benches);
