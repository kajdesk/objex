//! Throughput and latency benchmark against a running objex server.
//!
//!   cargo run --release --example bench -- http://127.0.0.1:9000 <access key> <secret key> [options]
//!
//! Options:
//!   --trials N         measured trials per workload (default 3; the median is reported)
//!   --only NAME        run one workload: small, medium, large, slow
//!   --slow-readers N   concurrent throttled GETs in the slow-client scenario (default 256)
//!
//! Each workload runs a warmup, then N trials of PUT and GET. Reported: median
//! throughput across trials and p50/p95/p99/max latency over all measured requests.
//! GET bodies are consumed as a stream and counted, never collected in memory.
//!
//! The "slow" scenario measures HEAD and small-PUT latency on an idle server and
//! again while many clients download slowly. Readers that tie up server threads show
//! up as a large p99 gap between the two.
//!
//! For meaningful numbers run the server with `RUST_LOG=warn` (no access log) and,
//! ideally, the client on a different host. Durable (fsync) and no-fsync modes are
//! different products; benchmark them separately.

use std::sync::Arc;
use std::time::{Duration, Instant};

use aws_credential_types::Credentials;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{BehaviorVersion, Region};
use aws_sdk_s3::primitives::ByteStream;
use tokio::sync::Semaphore;

const BUCKET: &str = "bench";

struct Opts {
    trials: usize,
    only: Option<String>,
    slow_readers: usize,
}

#[derive(Default)]
struct Stats {
    latencies: Vec<Duration>,
    errors: usize,
}

impl Stats {
    fn pct(&mut self, p: f64) -> Duration {
        if self.latencies.is_empty() {
            return Duration::ZERO;
        }
        self.latencies.sort_unstable();
        let i = ((self.latencies.len() as f64 * p).ceil() as usize).clamp(1, self.latencies.len()) - 1;
        self.latencies[i]
    }

    fn summary(&mut self) -> String {
        let ms = |d: Duration| d.as_secs_f64() * 1000.0;
        let (p50, p95, p99, max) = (self.pct(0.50), self.pct(0.95), self.pct(0.99), self.pct(1.0));
        let errs = if self.errors > 0 { format!("  ERRORS: {}", self.errors) } else { String::new() };
        format!("p50 {:>7.2} ms  p95 {:>7.2}  p99 {:>7.2}  max {:>7.2}{errs}", ms(p50), ms(p95), ms(p99), ms(max))
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Phase {
    Put,
    Get,
}

/// Read a GET body to the end, returning its length.
async fn drain(mut body: ByteStream) -> Result<usize, String> {
    let mut n = 0;
    while let Some(chunk) = body.next().await {
        n += chunk.map_err(|e| e.to_string())?.len();
    }
    Ok(n)
}

/// Run `count` operations with bounded concurrency; returns elapsed time and latencies.
async fn phase(c: &Client, phase: Phase, prefix: &str, data: &bytes::Bytes, count: usize, concurrency: usize) -> (Duration, Stats) {
    let sem = Arc::new(Semaphore::new(concurrency));
    let start = Instant::now();
    let mut tasks = tokio::task::JoinSet::new();
    for i in 0..count {
        // Acquire before spawning so at most `concurrency` tasks exist at once.
        let permit = sem.clone().acquire_owned().await.unwrap();
        let (c, data, key) = (c.clone(), data.clone(), format!("{prefix}/{i}"));
        tasks.spawn(async move {
            let _permit = permit;
            let t = Instant::now();
            let ok = match phase {
                Phase::Put => c.put_object().bucket(BUCKET).key(key).body(ByteStream::from(data)).send().await.is_ok(),
                Phase::Get => match c.get_object().bucket(BUCKET).key(key).send().await {
                    Ok(o) => drain(o.body).await.map(|n| n == data.len()).unwrap_or(false),
                    Err(_) => false,
                },
            };
            (t.elapsed(), ok)
        });
    }
    let mut stats = Stats::default();
    while let Some(r) = tasks.join_next().await {
        let (lat, ok) = r.unwrap();
        stats.latencies.push(lat);
        stats.errors += usize::from(!ok);
    }
    (start.elapsed(), stats)
}

async fn workload(c: &Client, o: &Opts, label: &str, count: usize, size: usize, concurrency: usize) {
    let data = bytes::Bytes::from((0..size).map(|i| (i * 7) as u8).collect::<Vec<_>>());
    // Warmup: populate connections and caches.
    let warm = (count / 10).clamp(concurrency.min(count), 200);
    phase(c, Phase::Put, &format!("{label}/warm"), &data, warm, concurrency).await;
    phase(c, Phase::Get, &format!("{label}/warm"), &data, warm, concurrency).await;

    for (p, name) in [(Phase::Put, "PUT"), (Phase::Get, "GET")] {
        let mut rates = Vec::new();
        let mut all = Stats::default();
        for trial in 0..o.trials {
            let (elapsed, stats) = phase(c, p, &format!("{label}/t{trial}"), &data, count, concurrency).await;
            rates.push(count as f64 / elapsed.as_secs_f64());
            all.latencies.extend(stats.latencies);
            all.errors += stats.errors;
        }
        rates.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let ops = rates[rates.len() / 2];
        println!(
            "{label:>7} {name}: {count} x {:>6} KiB, c={concurrency:<3} {ops:>8.0} ops/s {:>8.1} MiB/s   {}",
            size / 1024,
            ops * size as f64 / 1048576.0,
            all.summary()
        );
    }
}

/// HEAD and small-PUT latency, measured with 16 concurrent clients.
async fn probe(c: &Client, label: &str) {
    let small = bytes::Bytes::from_static(&[7u8; 4096]);
    let (_, mut heads) = {
        let sem = Arc::new(Semaphore::new(16));
        let start = Instant::now();
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..2000 {
            let permit = sem.clone().acquire_owned().await.unwrap();
            let c = c.clone();
            tasks.spawn(async move {
                let _p = permit;
                let t = Instant::now();
                let ok = c.head_object().bucket(BUCKET).key("slow/probe").send().await.is_ok();
                (t.elapsed(), ok)
            });
        }
        let mut s = Stats::default();
        while let Some(r) = tasks.join_next().await {
            let (l, ok) = r.unwrap();
            s.latencies.push(l);
            s.errors += usize::from(!ok);
        }
        (start.elapsed(), s)
    };
    let (_, mut puts) = phase(c, Phase::Put, &format!("slow/probe-put-{label}"), &small, 500, 16).await;
    println!("   slow {label:<16} HEAD {}", heads.summary());
    println!("   slow {label:<16} PUT  {}", puts.summary());
}

/// Latency of cheap requests on an idle server vs. with many slow downloads active.
async fn slow_clients(c: &Client, o: &Opts) {
    let big = bytes::Bytes::from(vec![1u8; 64 * 1024 * 1024]);
    c.put_object().bucket(BUCKET).key("slow/big").body(ByteStream::from(big)).send().await.expect("put slow/big");
    c.put_object().bucket(BUCKET).key("slow/probe").body(ByteStream::from_static(b"x")).send().await.expect("put probe");
    probe(c, "idle").await;

    let stop = Arc::new(tokio::sync::Notify::new());
    let mut readers = tokio::task::JoinSet::new();
    for _ in 0..o.slow_readers {
        let (c, stop) = (c.clone(), stop.clone());
        readers.spawn(async move {
            let Ok(o) = c.get_object().bucket(BUCKET).key("slow/big").send().await else { return };
            let mut body = o.body;
            // Read ~16 KiB every 50 ms: a client on a very slow link.
            loop {
                tokio::select! {
                    _ = stop.notified() => return,
                    chunk = body.next() => match chunk {
                        Some(Ok(_)) => tokio::time::sleep(Duration::from_millis(50)).await,
                        _ => return,
                    },
                }
            }
        });
    }
    tokio::time::sleep(Duration::from_secs(2)).await; // let the readers settle
    probe(c, &format!("{} readers", o.slow_readers)).await;
    stop.notify_waiters();
    readers.abort_all();
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 4 {
        eprintln!("usage: bench <endpoint> <access key> <secret key> [--trials N] [--only small|medium|large|slow] [--slow-readers N]");
        std::process::exit(2);
    }
    let mut o = Opts { trials: 3, only: None, slow_readers: 256 };
    let mut rest = args[4..].iter();
    while let Some(a) = rest.next() {
        let mut val = || rest.next().expect("missing option value").clone();
        match a.as_str() {
            "--trials" => o.trials = val().parse().expect("--trials"),
            "--only" => o.only = Some(val()),
            "--slow-readers" => o.slow_readers = val().parse().expect("--slow-readers"),
            other => panic!("unknown option {other}"),
        }
    }
    let conf = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .endpoint_url(&args[1])
        .region(Region::new("auto"))
        .credentials_provider(Credentials::new(&args[2], &args[3], None, None, "bench"))
        .force_path_style(true)
        .build();
    let c = Client::from_conf(conf);
    let _ = c.create_bucket().bucket(BUCKET).send().await;
    let run = |name: &str| o.only.as_deref().is_none_or(|x| x == name);
    if run("small") {
        workload(&c, &o, "small", 5000, 16 * 1024, 64).await; // thumbnails, log chunks
    }
    if run("medium") {
        workload(&c, &o, "medium", 400, 1024 * 1024, 32).await; // photos
    }
    if run("large") {
        workload(&c, &o, "large", 16, 64 * 1024 * 1024, 8).await; // video
    }
    if run("slow") {
        slow_clients(&c, &o).await;
    }
}
