//! Asynchronous reclamation of blobs that metadata no longer references.
//!
//! Requests acknowledge as soon as metadata commits; the blob files are deleted
//! afterwards by a background task, with bounded concurrency. Blobs are held back
//! while a reader has them leased (a GET opens segments lazily, so it must keep
//! even an overwritten object's blobs until it finishes), and for a short delay
//! that covers the gap between a reader's metadata snapshot and taking its lease.
//!
//! The queue is in memory. Anything lost in a crash is unreferenced and old, which
//! is exactly what the garbage collector removes.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

use super::super::{BlobRef, BlobStore};

/// Blobs held open by readers, counted per blob id.
#[derive(Default)]
pub struct Leases(Mutex<HashMap<String, usize>>);

impl Leases {
    pub fn acquire(self: &Arc<Self>, ids: Vec<String>) -> LeaseGuard {
        let mut m = self.0.lock().unwrap();
        for id in &ids {
            *m.entry(id.clone()).or_default() += 1;
        }
        LeaseGuard { leases: self.clone(), ids }
    }

    pub fn is_leased(&self, id: &str) -> bool {
        self.0.lock().unwrap().contains_key(id)
    }
}

pub struct LeaseGuard {
    leases: Arc<Leases>,
    ids: Vec<String>,
}

impl Drop for LeaseGuard {
    fn drop(&mut self) {
        let mut m = self.leases.0.lock().unwrap();
        for id in &self.ids {
            if let Some(n) = m.get_mut(id) {
                *n -= 1;
                if *n == 0 {
                    m.remove(id);
                }
            }
        }
    }
}

enum Msg {
    Blobs(Vec<BlobRef>),
    /// Delete everything queued and unleased now, then reply.
    Flush(oneshot::Sender<()>),
}

/// Most blob deletions in flight at once.
const CONCURRENCY: usize = 16;
const TICK: Duration = Duration::from_millis(250);

pub struct Reclaimer {
    tx: mpsc::UnboundedSender<Msg>,
    /// The worker is started on first use, from inside a Tokio runtime.
    start: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

impl Reclaimer {
    pub fn new<B: BlobStore>(blobs: Arc<B>, leases: Arc<Leases>, delay: Duration) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let start: Box<dyn FnOnce() + Send> = Box::new(move || {
            tokio::spawn(run(blobs, leases, delay, rx));
        });
        Reclaimer { tx, start: Mutex::new(Some(start)) }
    }

    fn ensure_started(&self) {
        if let Some(start) = self.start.lock().unwrap().take() {
            start();
        }
    }

    /// Queue blobs for deletion.
    pub fn reclaim(&self, blobs: Vec<BlobRef>) {
        if blobs.is_empty() {
            return;
        }
        self.ensure_started();
        let _ = self.tx.send(Msg::Blobs(blobs));
    }

    /// Delete every queued blob that is not leased, ignoring the delay.
    pub async fn flush(&self) {
        self.ensure_started();
        let (done, wait) = oneshot::channel();
        if self.tx.send(Msg::Flush(done)).is_ok() {
            let _ = wait.await;
        }
    }
}

async fn run<B: BlobStore>(blobs: Arc<B>, leases: Arc<Leases>, delay: Duration, mut rx: mpsc::UnboundedReceiver<Msg>) {
    let mut queue: VecDeque<(Instant, BlobRef)> = VecDeque::new();
    let mut tick = tokio::time::interval(TICK);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        let flush = tokio::select! {
            m = rx.recv() => match m {
                None => return,
                Some(Msg::Blobs(v)) => {
                    let now = Instant::now();
                    queue.extend(v.into_iter().map(|b| (now, b)));
                    continue;
                }
                Some(Msg::Flush(done)) => Some(done),
            },
            _ = tick.tick() => None,
        };
        let now = Instant::now();
        let mut due = Vec::new();
        while let Some((t, _)) = queue.front()
            && (flush.is_some() || now.duration_since(*t) >= delay)
        {
            due.push(queue.pop_front().unwrap().1);
        }
        let mut tasks = tokio::task::JoinSet::new();
        for b in due {
            if leases.is_leased(&b.id) {
                // Still being read: try again later.
                queue.push_back((now, b));
                continue;
            }
            if tasks.len() >= CONCURRENCY {
                tasks.join_next().await;
            }
            let blobs = blobs.clone();
            tasks.spawn(async move {
                if let Err(e) = blobs.delete(&b).await {
                    // Left for the garbage collector.
                    tracing::warn!("deleting blob {}: {e}", b.id);
                }
            });
        }
        tasks.join_all().await;
        if let Some(done) = flush {
            let _ = done.send(());
        }
    }
}
