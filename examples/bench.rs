//! Rough throughput benchmark against a running objex server.
//!
//!   cargo run --release --example bench -- http://127.0.0.1:9000 <access key> <secret key>

use std::sync::Arc;
use std::time::Instant;

use aws_credential_types::Credentials;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{BehaviorVersion, Region};
use aws_sdk_s3::primitives::ByteStream;
use tokio::sync::Semaphore;

async fn run(c: &Client, label: &str, count: usize, size: usize, concurrency: usize) {
    let data = bytes::Bytes::from((0..size).map(|i| (i * 7) as u8).collect::<Vec<_>>());
    let sem = Arc::new(Semaphore::new(concurrency));
    for phase in ["PUT", "GET"] {
        let start = Instant::now();
        let mut tasks = Vec::new();
        for i in 0..count {
            let (c, data, sem) = (c.clone(), data.clone(), sem.clone());
            let key = format!("{label}/{i}");
            tasks.push(tokio::spawn(async move {
                let _p = sem.acquire().await.unwrap();
                if phase == "PUT" {
                    c.put_object().bucket("bench").key(key).body(ByteStream::from(data)).send().await.unwrap();
                } else {
                    let o = c.get_object().bucket("bench").key(key).send().await.unwrap();
                    assert_eq!(o.body.collect().await.unwrap().into_bytes().len(), data.len());
                }
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
        let secs = start.elapsed().as_secs_f64();
        println!(
            "{label:>8} {phase}: {count} x {} KiB, {concurrency} parallel: {:>8.0} ops/s {:>8.1} MiB/s",
            size / 1024,
            count as f64 / secs,
            (count * size) as f64 / secs / 1048576.0
        );
    }
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let conf = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .endpoint_url(&args[1])
        .region(Region::new("auto"))
        .credentials_provider(Credentials::new(&args[2], &args[3], None, None, "bench"))
        .force_path_style(true)
        .build();
    let c = Client::from_conf(conf);
    let _ = c.create_bucket().bucket("bench").send().await;
    run(&c, "small", 5000, 16 * 1024, 64).await; // rrweb chunks, thumbnails
    run(&c, "medium", 400, 1024 * 1024, 32).await; // photos
    run(&c, "large", 16, 64 * 1024 * 1024, 8).await; // video
}
