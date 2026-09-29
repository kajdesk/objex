//! HTTP server: accept loop, graceful shutdown, key hot-reload and background GC.

use std::convert::Infallible;
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use hyper_util::server::graceful::GracefulShutdown;
use tokio::net::TcpListener;

use crate::config::Config;
use crate::s3::{self, AppState};
use crate::storage::local::LocalEngine;

/// How long to wait for in-flight requests on shutdown.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(30);
/// Unreferenced blobs younger than this are left alone by GC.
const GC_GRACE: Duration = Duration::from_secs(3600);
const GC_INTERVAL: Duration = Duration::from_secs(3600);
const KEY_RELOAD_INTERVAL: Duration = Duration::from_secs(2);

/// Serve S3 requests on `listener` until `shutdown` resolves, then drain connections.
pub async fn serve(listener: TcpListener, state: Arc<AppState>, shutdown: impl Future<Output = ()>) {
    let builder = auto::Builder::new(TokioExecutor::new());
    let graceful = GracefulShutdown::new();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, peer) = match accepted {
                    Ok(a) => a,
                    Err(e) => {
                        tracing::warn!("accept: {e}");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                };
                let _ = stream.set_nodelay(true);
                let state = state.clone();
                let svc = service_fn(move |req: hyper::Request<hyper::body::Incoming>| {
                    let state = state.clone();
                    async move {
                        let started = Instant::now();
                        let (method, path) = (req.method().clone(), req.uri().path().to_string());
                        let resp = s3::handle(state, req).await;
                        tracing::info!(target: "objex::access", %peer, %method, path, status = resp.status().as_u16(), ms = started.elapsed().as_millis() as u64);
                        Ok::<_, Infallible>(resp)
                    }
                });
                let conn = graceful.watch(builder.serve_connection(TokioIo::new(stream), svc).into_owned());
                tokio::spawn(async move {
                    if let Err(e) = conn.await {
                        tracing::debug!("connection from {peer}: {e}");
                    }
                });
            }
            _ = &mut shutdown => break,
        }
    }
    drop(listener);
    tokio::select! {
        _ = graceful.shutdown() => {}
        _ = tokio::time::sleep(DRAIN_TIMEOUT) => tracing::warn!("timed out waiting for connections to close"),
    }
}

/// Resolves on Ctrl-C or SIGTERM.
pub async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let term = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {}
        _ = term => {}
    }
    tracing::info!("shutting down");
}

/// Re-read access keys whenever the config file changes.
fn spawn_key_reload(state: Arc<AppState>, path: PathBuf) {
    tokio::spawn(async move {
        let mtime = |p: &PathBuf| std::fs::metadata(p).and_then(|m| m.modified()).ok();
        let mut last: Option<SystemTime> = mtime(&path);
        loop {
            tokio::time::sleep(KEY_RELOAD_INTERVAL).await;
            let now = mtime(&path);
            if now == last {
                continue;
            }
            last = now;
            match Config::load_keys(&path) {
                Ok(keys) => {
                    tracing::info!("reloaded {} access key(s) from {}", keys.len(), path.display());
                    state.set_keys(keys);
                }
                Err(e) => tracing::warn!("not reloading keys: {e}"),
            }
        }
    });
}

fn spawn_gc(engine: Arc<LocalEngine>) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(GC_INTERVAL).await;
            match engine.gc(GC_GRACE).await {
                Ok(0) => {}
                Ok(n) => tracing::info!("gc: removed {n} unreferenced blob(s)"),
                Err(e) => tracing::warn!("gc: {e}"),
            }
        }
    });
}

/// Run the server described by `cfg` until a shutdown signal.
pub async fn run(cfg: Config, config_path: PathBuf) -> Result<(), String> {
    let engine = Arc::new(LocalEngine::open(&cfg.data_dir, cfg.fsync).map_err(|e| format!("opening {}: {e}", cfg.data_dir.display()))?);
    if cfg.keys.is_empty() {
        tracing::warn!("no access keys configured: only anonymous reads of public buckets will work. Create one with `objex key add <name>`");
    }
    let state = Arc::new(AppState::new(engine.clone(), cfg.keys.clone(), cfg.region.clone(), cfg.domain.clone()));
    spawn_key_reload(state.clone(), config_path);
    spawn_gc(engine);
    let listener = TcpListener::bind(&cfg.listen).await.map_err(|e| format!("binding {}: {e}", cfg.listen))?;
    tracing::info!(
        "objex listening on {} (data: {}, fsync: {}{})",
        listener.local_addr().map(|a| a.to_string()).unwrap_or(cfg.listen.clone()),
        cfg.data_dir.display(),
        cfg.fsync,
        if cfg.domain.is_empty() { String::new() } else { format!(", virtual hosts: *.{}", cfg.domain) }
    );
    serve(listener, state, shutdown_signal()).await;
    Ok(())
}
