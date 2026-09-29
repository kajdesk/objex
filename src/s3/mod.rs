//! The S3 HTTP API: request parsing, routing, authorization and responses.

mod bucket;
mod cors;
mod multipart;
mod object;

use std::io;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};

use bytes::Bytes;
use chrono::Utc;
use http::request::Parts;
use http::{HeaderMap, HeaderValue, Method, Request, Response, StatusCode};
use http_body::{Body, Frame};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Empty, Full};
use tokio::sync::mpsc;

use crate::auth::{self, Auth, Payload};
use crate::body::{ChunkedSource, PlainSource, read_limited};
use crate::checksum::{Checksum, ChecksumAlgo};
use crate::config::KeyConfig;
use crate::error::{ErrorCode, S3Error, S3Result};
use crate::storage::{ByteSource, ObjectLayer, PutExpect};
use crate::util::{random_hex, uri_decode};
use crate::xml::XmlWriter;

pub type RespBody = BoxBody<Bytes, io::Error>;
pub type Resp = Response<RespBody>;

/// Largest XML request body accepted (DeleteObjects with 1000 long keys fits).
const MAX_XML_BODY: usize = 4 * 1024 * 1024;

/// Shared server state.
pub struct AppState {
    pub store: Arc<dyn ObjectLayer>,
    keys: RwLock<Arc<Vec<KeyConfig>>>,
    /// Region reported to clients.
    pub region: String,
    /// Base domain for virtual-host style requests; empty disables them.
    pub domain: String,
}

impl AppState {
    pub fn new(store: Arc<dyn ObjectLayer>, keys: Vec<KeyConfig>, region: String, domain: String) -> Self {
        AppState { store, keys: RwLock::new(Arc::new(keys)), region, domain: domain.trim_matches('.').to_ascii_lowercase() }
    }

    pub fn keys(&self) -> Arc<Vec<KeyConfig>> {
        self.keys.read().unwrap().clone()
    }

    pub fn set_keys(&self, keys: Vec<KeyConfig>) {
        *self.keys.write().unwrap() = Arc::new(keys);
    }
}

// ---------------------------------------------------------------------------
// Response bodies
// ---------------------------------------------------------------------------

pub fn full(b: impl Into<Bytes>) -> RespBody {
    Full::new(b.into()).map_err(|never| match never {}).boxed()
}

pub fn empty_body() -> RespBody {
    Empty::new().map_err(|never| match never {}).boxed()
}

/// Streams object data produced by a blocking reader thread.
struct ChannelBody(mpsc::Receiver<io::Result<Bytes>>);

impl Body for ChannelBody {
    type Data = Bytes;
    type Error = io::Error;

    fn poll_frame(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Frame<Bytes>, io::Error>>> {
        self.0.poll_recv(cx).map(|o| o.map(|r| r.map(Frame::data)))
    }
}

pub fn stream_body(rx: mpsc::Receiver<io::Result<Bytes>>) -> RespBody {
    ChannelBody(rx).boxed()
}

fn empty(status: StatusCode) -> Resp {
    let mut r = Response::new(empty_body());
    *r.status_mut() = status;
    r
}

fn xml(body: String) -> Resp {
    let mut r = Response::new(full(body));
    r.headers_mut().insert("content-type", HeaderValue::from_static("application/xml"));
    r
}

fn set(h: &mut HeaderMap, name: &'static str, value: impl AsRef<str>) {
    if let Ok(v) = HeaderValue::from_str(value.as_ref()) {
        h.insert(name, v);
    }
}

fn set_checksum(h: &mut HeaderMap, c: Option<&Checksum>) {
    if let Some(c) = c {
        set(h, c.algo.header(), &c.value);
        set(h, "x-amz-checksum-type", crate::checksum::ChecksumType::of(c).name());
    }
}

fn error_response(e: &S3Error, resource: &str, request_id: &str, head: bool) -> Resp {
    let status = e.code.status();
    if head || status == StatusCode::NOT_MODIFIED {
        return empty(status);
    }
    let mut w = XmlWriter::bare("Error");
    w.elem("Code", e.code.as_str()).elem("Message", &e.message).elem("Resource", resource).elem("RequestId", request_id);
    let mut r = xml(w.finish());
    *r.status_mut() = status;
    r
}

// ---------------------------------------------------------------------------
// Request context
// ---------------------------------------------------------------------------

pub struct Ctx<B> {
    pub state: Arc<AppState>,
    pub parts: Parts,
    body: Option<B>,
    pub query: Vec<(String, String)>,
    /// Empty for service-level requests.
    pub bucket: String,
    /// Empty for bucket-level requests.
    pub key: String,
    pub auth: Auth,
}

impl<B> Ctx<B> {
    pub fn q(&self, name: &str) -> Option<&str> {
        self.query.iter().find(|(k, _)| k == name).map(|(_, v)| v.as_str())
    }

    pub fn has(&self, name: &str) -> bool {
        self.q(name).is_some()
    }

    pub fn header(&self, name: &str) -> Option<&str> {
        self.parts.headers.get(name).and_then(|v| v.to_str().ok())
    }

    fn content_length(&self) -> Option<u64> {
        self.header("content-length").and_then(|v| v.parse().ok())
    }

    /// Checksum and digest expectations for an upload, from the request headers.
    pub fn put_expect(&self) -> S3Result<PutExpect> {
        let mut e = PutExpect::default();
        if let Some(md5) = self.header("content-md5") {
            use base64::Engine as _;
            let raw = base64::engine::general_purpose::STANDARD.decode(md5.trim()).map_err(|_| ErrorCode::InvalidDigest)?;
            e.content_md5 = Some(raw.try_into().map_err(|_| ErrorCode::InvalidDigest)?);
        }
        for algo in ChecksumAlgo::ALL {
            if let Some(v) = self.header(algo.header()) {
                if e.checksum.is_some() {
                    return Err(S3Error::msg(ErrorCode::InvalidRequest, "Expecting a single x-amz-checksum- header. Multiple checksum Types are not allowed."));
                }
                e.checksum = Some(Checksum { algo, value: v.trim().to_string() });
            }
        }
        let named = self.header("x-amz-sdk-checksum-algorithm").or(self.header("x-amz-checksum-algorithm"));
        if let Some(a) = named {
            e.checksum_algo = Some(ChecksumAlgo::parse(a).ok_or_else(|| S3Error::msg(ErrorCode::InvalidRequest, "Invalid checksum algorithm"))?);
        }
        if let Some(t) = self.header("x-amz-trailer") {
            e.checksum_algo = Some(ChecksumAlgo::from_header(t.trim()).ok_or_else(|| S3Error::msg(ErrorCode::InvalidRequest, format!("Unsupported trailer {t}")))?);
        }
        if let (Some(c), Some(a)) = (&e.checksum, e.checksum_algo)
            && c.algo != a
        {
            return Err(S3Error::msg(ErrorCode::InvalidRequest, "Value for x-amz-checksum-algorithm header is invalid."));
        }
        match &self.auth.payload {
            Payload::Streaming { .. } => {
                let n = self.header("x-amz-decoded-content-length").ok_or(ErrorCode::MissingContentLength)?;
                e.size = Some(n.parse().map_err(|_| S3Error::msg(ErrorCode::InvalidArgument, "Invalid x-amz-decoded-content-length"))?);
            }
            Payload::Sha256(h) => {
                e.sha256 = Some(*h);
                e.size = self.content_length();
            }
            Payload::Unsigned => e.size = self.content_length(),
        }
        if let Some(n) = e.size
            && n > crate::storage::MAX_PUT_SIZE
        {
            return Err(ErrorCode::EntityTooLarge.into());
        }
        Ok(e)
    }

    /// Host the client addressed, for Location values.
    pub fn host(&self) -> String {
        self.header("host").map(str::to_string).or_else(|| self.parts.uri.authority().map(|a| a.to_string())).unwrap_or_default()
    }
}

impl<B> Ctx<B>
where
    B: Body<Data = Bytes> + Unpin + Send + 'static,
    B::Error: std::fmt::Display,
{
    /// The request body as a stream, decoding aws-chunked when used.
    pub fn source(&mut self) -> Box<dyn ByteSource> {
        let body = self.body.take().expect("request body taken twice");
        match &self.auth.payload {
            Payload::Streaming { signer, trailer } => Box::new(ChunkedSource::new(body, signer.clone(), *trailer)),
            _ => Box::new(PlainSource::new(body)),
        }
    }

    /// Read a small (XML) request body, verifying its SHA-256 and Content-MD5.
    pub async fn read_body(&mut self) -> S3Result<Bytes> {
        let data = match &self.auth.payload {
            Payload::Streaming { .. } => {
                let mut src = self.source();
                let mut out = Vec::new();
                while let Some(c) = src.next_chunk().await? {
                    if out.len() + c.len() > MAX_XML_BODY {
                        return Err(S3Error::msg(ErrorCode::InvalidRequest, "Request body is too large"));
                    }
                    out.extend_from_slice(&c);
                }
                Bytes::from(out)
            }
            payload => {
                let payload = payload.clone();
                let mut body = self.body.take().expect("request body taken twice");
                let data = read_limited(&mut body, MAX_XML_BODY).await?;
                if let Payload::Sha256(h) = payload {
                    use sha2::Digest;
                    if sha2::Sha256::digest(&data)[..] != h[..] {
                        return Err(ErrorCode::XAmzContentSHA256Mismatch.into());
                    }
                }
                data
            }
        };
        if let Some(md5) = self.header("content-md5") {
            use base64::Engine as _;
            use md5::Digest;
            let want = base64::engine::general_purpose::STANDARD.decode(md5.trim()).map_err(|_| ErrorCode::InvalidDigest)?;
            if md5::Md5::digest(&data)[..] != want[..] {
                return Err(ErrorCode::BadDigest.into());
            }
        }
        Ok(data)
    }
}

/// Owner reported in listings and ACLs. objex has a single tenant.
pub fn write_owner(w: &mut XmlWriter, tag: &'static str) {
    w.open(tag).elem("ID", "objex").elem("DisplayName", "objex").close();
}

// ---------------------------------------------------------------------------
// Routing
// ---------------------------------------------------------------------------

/// Split a request into (bucket, key), for path-style and virtual-host style addressing.
fn split_path(state: &AppState, parts: &Parts) -> S3Result<(String, String)> {
    let path = parts.uri.path();
    let decode = |s: &str| uri_decode(s).ok_or_else(|| S3Error::msg(ErrorCode::InvalidArgument, "Invalid URI encoding"));
    if !state.domain.is_empty() {
        let host = parts
            .headers
            .get("host")
            .and_then(|h| h.to_str().ok())
            .map(str::to_string)
            .or_else(|| parts.uri.authority().map(|a| a.to_string()))
            .unwrap_or_default()
            .to_ascii_lowercase();
        let host = strip_port(&host);
        if let Some(b) = host.strip_suffix(state.domain.as_str()).and_then(|h| h.strip_suffix('.'))
            && !b.is_empty()
        {
            let key = path.strip_prefix('/').unwrap_or(path);
            return Ok((b.to_string(), decode(key)?));
        }
    }
    let p = path.strip_prefix('/').unwrap_or(path);
    let (b, k) = p.split_once('/').unwrap_or((p, ""));
    Ok((decode(b)?, decode(k)?))
}

fn strip_port(host: &str) -> &str {
    if host.starts_with('[') {
        return host.split_once(']').map(|(h, _)| &host[..h.len() + 1]).unwrap_or(host);
    }
    host.rsplit_once(':').filter(|(_, p)| p.chars().all(|c| c.is_ascii_digit())).map(|(h, _)| h).unwrap_or(host)
}

#[derive(Debug)]
enum Op {
    ListBuckets,
    CreateBucket,
    HeadBucket,
    DeleteBucket,
    ListObjects,
    GetBucketLocation,
    GetBucketAcl,
    PutBucketAcl,
    GetBucketCors,
    PutBucketCors,
    DeleteBucketCors,
    GetBucketVersioning,
    ListUploads,
    DeleteObjects,
    GetObject,
    HeadObject,
    PutObject,
    CopyObject { src_bucket: String, src_key: String },
    DeleteObject,
    GetObjectAcl,
    PutObjectAcl,
    GetObjectTagging,
    CreateMultipart,
    UploadPart,
    UploadPartCopy { src_bucket: String, src_key: String },
    CompleteMultipart,
    AbortMultipart,
    ListParts,
    /// A configuration S3 supports but objex does not; reported as absent.
    Absent(ErrorCode),
    /// Accepted and ignored.
    NoContent,
}

impl Op {
    /// Operations anonymous clients may perform on public-read buckets.
    fn public_read(&self) -> bool {
        matches!(self, Op::GetObject | Op::HeadObject | Op::ListObjects)
    }
}

/// Parse "x-amz-copy-source" ("/bucket/key", "bucket/key", URL-encoded, optional versionId).
fn parse_copy_source(s: &str) -> S3Result<(String, String)> {
    let invalid = || S3Error::msg(ErrorCode::InvalidArgument, "Copy Source must mention the source bucket and key: sourcebucket/sourcekey");
    let s = s.split_once("?versionId=").map(|(p, _)| p).unwrap_or(s);
    let d = uri_decode(s).ok_or_else(invalid)?;
    let d = d.strip_prefix('/').unwrap_or(&d);
    let (b, k) = d.split_once('/').ok_or_else(invalid)?;
    if b.is_empty() || k.is_empty() {
        return Err(invalid());
    }
    Ok((b.to_string(), k.to_string()))
}

fn route<B>(ctx: &Ctx<B>) -> S3Result<Op> {
    let m = &ctx.parts.method;
    let not_impl = || Err(ErrorCode::NotImplemented.into());
    if ctx.bucket.is_empty() {
        return match *m {
            Method::GET => Ok(Op::ListBuckets),
            _ => Err(ErrorCode::MethodNotAllowed.into()),
        };
    }
    if ctx.key.is_empty() {
        let has = |n: &str| ctx.has(n);
        return match *m {
            Method::GET if has("location") => Ok(Op::GetBucketLocation),
            Method::GET if has("acl") => Ok(Op::GetBucketAcl),
            Method::GET if has("cors") => Ok(Op::GetBucketCors),
            Method::GET if has("versioning") => Ok(Op::GetBucketVersioning),
            Method::GET if has("uploads") => Ok(Op::ListUploads),
            Method::GET if has("policy") => Ok(Op::Absent(ErrorCode::NoSuchBucketPolicy)),
            Method::GET if has("lifecycle") => Ok(Op::Absent(ErrorCode::NoSuchLifecycleConfiguration)),
            Method::GET if has("tagging") => Ok(Op::Absent(ErrorCode::NoSuchTagSet)),
            Method::GET if has("encryption") => Ok(Op::Absent(ErrorCode::ServerSideEncryptionConfigurationNotFoundError)),
            Method::GET if ["website", "logging", "notification", "replication", "object-lock", "ownershipControls", "requestPayment", "accelerate", "analytics", "inventory", "metrics", "intelligent-tiering", "publicAccessBlock", "policyStatus", "versions"].iter().any(|n| has(n)) => not_impl(),
            Method::GET => Ok(Op::ListObjects),
            Method::HEAD => Ok(Op::HeadBucket),
            Method::PUT if has("acl") => Ok(Op::PutBucketAcl),
            Method::PUT if has("cors") => Ok(Op::PutBucketCors),
            Method::PUT if ctx.query.is_empty() => Ok(Op::CreateBucket),
            Method::PUT => not_impl(),
            Method::DELETE if has("cors") => Ok(Op::DeleteBucketCors),
            Method::DELETE if has("policy") || has("lifecycle") || has("tagging") || has("encryption") => Ok(Op::NoContent),
            Method::DELETE if ctx.query.is_empty() => Ok(Op::DeleteBucket),
            Method::DELETE => not_impl(),
            Method::POST if has("delete") => Ok(Op::DeleteObjects),
            Method::POST => not_impl(),
            _ => Err(ErrorCode::MethodNotAllowed.into()),
        };
    }
    let has = |n: &str| ctx.has(n);
    let copy_source = ctx.header("x-amz-copy-source").map(parse_copy_source).transpose()?;
    match *m {
        Method::GET if has("uploadId") => Ok(Op::ListParts),
        Method::GET if has("acl") => Ok(Op::GetObjectAcl),
        Method::GET if has("tagging") => Ok(Op::GetObjectTagging),
        Method::GET if has("attributes") || has("retention") || has("legal-hold") || has("torrent") => not_impl(),
        Method::GET => Ok(Op::GetObject),
        Method::HEAD => Ok(Op::HeadObject),
        Method::PUT if has("uploadId") || has("partNumber") => match copy_source {
            Some((src_bucket, src_key)) => Ok(Op::UploadPartCopy { src_bucket, src_key }),
            None => Ok(Op::UploadPart),
        },
        Method::PUT if has("acl") => Ok(Op::PutObjectAcl),
        Method::PUT if has("tagging") || has("retention") || has("legal-hold") => not_impl(),
        Method::PUT => match copy_source {
            Some((src_bucket, src_key)) => Ok(Op::CopyObject { src_bucket, src_key }),
            None => Ok(Op::PutObject),
        },
        Method::DELETE if has("uploadId") => Ok(Op::AbortMultipart),
        Method::DELETE if has("tagging") => Ok(Op::NoContent),
        Method::DELETE => Ok(Op::DeleteObject),
        Method::POST if has("uploads") => Ok(Op::CreateMultipart),
        Method::POST if has("uploadId") => Ok(Op::CompleteMultipart),
        Method::POST if has("select") || has("restore") => not_impl(),
        _ => Err(ErrorCode::MethodNotAllowed.into()),
    }
}

async fn authorize<B>(ctx: &Ctx<B>, op: &Op) -> S3Result<()> {
    let Some(key) = &ctx.auth.key else {
        if op.public_read() && ctx.state.store.get_bucket(&ctx.bucket).await?.public_read {
            return Ok(());
        }
        return Err(ErrorCode::AccessDenied.into());
    };
    let write = !matches!(ctx.parts.method, Method::GET | Method::HEAD);
    if write && key.read_only {
        return Err(ErrorCode::AccessDenied.into());
    }
    if !ctx.bucket.is_empty() && !key.can_access(&ctx.bucket) {
        return Err(ErrorCode::AccessDenied.into());
    }
    if let Op::CopyObject { src_bucket, .. } | Op::UploadPartCopy { src_bucket, .. } = op
        && !key.can_access(src_bucket)
    {
        return Err(ErrorCode::AccessDenied.into());
    }
    Ok(())
}

async fn execute<B>(ctx: Ctx<B>, op: Op) -> S3Result<Resp>
where
    B: Body<Data = Bytes> + Unpin + Send + 'static,
    B::Error: std::fmt::Display,
{
    match op {
        Op::ListBuckets => bucket::list_buckets(ctx).await,
        Op::CreateBucket => bucket::create(ctx).await,
        Op::HeadBucket => bucket::head(ctx).await,
        Op::DeleteBucket => bucket::delete(ctx).await,
        Op::ListObjects => bucket::list_objects(ctx).await,
        Op::GetBucketLocation => bucket::location(ctx).await,
        Op::GetBucketAcl => bucket::get_acl(ctx).await,
        Op::PutBucketAcl => bucket::put_acl(ctx).await,
        Op::GetBucketCors => bucket::get_cors(ctx).await,
        Op::PutBucketCors => bucket::put_cors(ctx).await,
        Op::DeleteBucketCors => bucket::delete_cors(ctx).await,
        Op::GetBucketVersioning => bucket::versioning(ctx).await,
        Op::DeleteObjects => bucket::delete_objects(ctx).await,
        Op::ListUploads => multipart::list_uploads(ctx).await,
        Op::GetObject => object::get(ctx, false).await,
        Op::HeadObject => object::get(ctx, true).await,
        Op::PutObject => object::put(ctx).await,
        Op::CopyObject { src_bucket, src_key } => object::copy(ctx, src_bucket, src_key).await,
        Op::DeleteObject => object::delete(ctx).await,
        Op::GetObjectAcl => object::get_acl(ctx).await,
        Op::PutObjectAcl => object::put_acl(ctx).await,
        Op::GetObjectTagging => object::get_tagging(ctx).await,
        Op::CreateMultipart => multipart::create(ctx).await,
        Op::UploadPart => multipart::upload_part(ctx).await,
        Op::UploadPartCopy { src_bucket, src_key } => multipart::upload_part_copy(ctx, src_bucket, src_key).await,
        Op::CompleteMultipart => multipart::complete(ctx).await,
        Op::AbortMultipart => multipart::abort(ctx).await,
        Op::ListParts => multipart::list_parts(ctx).await,
        Op::Absent(code) => {
            ctx.state.store.get_bucket(&ctx.bucket).await?;
            Err(code.into())
        }
        Op::NoContent => {
            if ctx.key.is_empty() {
                ctx.state.store.get_bucket(&ctx.bucket).await?;
            }
            Ok(empty(StatusCode::NO_CONTENT))
        }
    }
}

async fn process<B>(state: Arc<AppState>, parts: Parts, body: B, bucket: String, key: String) -> S3Result<Resp>
where
    B: Body<Data = Bytes> + Unpin + Send + 'static,
    B::Error: std::fmt::Display,
{
    if parts.method == Method::OPTIONS {
        return cors::preflight(&state, &parts, &bucket).await;
    }
    let query = auth::parse_query(parts.uri.query());
    let keys = state.keys();
    let auth = auth::authenticate(&parts, &query, &keys, Utc::now())?;
    let ctx = Ctx { state, parts, body: Some(body), query, bucket, key, auth };
    let op = route(&ctx)?;
    tracing::debug!(?op, bucket = %ctx.bucket, key = %ctx.key, "request");
    authorize(&ctx, &op).await?;
    execute(ctx, op).await
}

/// Handle one HTTP request.
pub async fn handle<B>(state: Arc<AppState>, req: Request<B>) -> Resp
where
    B: Body<Data = Bytes> + Unpin + Send + 'static,
    B::Error: std::fmt::Display,
{
    let request_id = random_hex(8).to_uppercase();
    let (parts, body) = req.into_parts();
    let method = parts.method.clone();
    let resource = parts.uri.path().to_string();
    let origin = parts.headers.get("origin").and_then(|v| v.to_str().ok()).map(str::to_string);

    let (bucket, result) = match split_path(&state, &parts) {
        Ok((bucket, key)) => (bucket.clone(), process(state.clone(), parts, body, bucket, key).await),
        Err(e) => (String::new(), Err(e)),
    };
    let mut resp = match result {
        Ok(r) => r,
        Err(e) => {
            if e.code.status().is_server_error() {
                tracing::error!("{method} {resource}: {e}");
            } else {
                tracing::debug!("{method} {resource}: {e}");
            }
            error_response(&e, &resource, &request_id, method == Method::HEAD)
        }
    };
    if let Some(origin) = origin
        && !bucket.is_empty()
        && method != Method::OPTIONS
        && let Ok(info) = state.store.get_bucket(&bucket).await
    {
        cors::apply(&info, &origin, method.as_str(), resp.headers_mut());
    }
    let h = resp.headers_mut();
    set(h, "x-amz-request-id", &request_id);
    h.insert("server", HeaderValue::from_static("objex"));
    resp
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn copy_source() {
        assert_eq!(parse_copy_source("/b/k%20x").unwrap(), ("b".into(), "k x".into()));
        assert_eq!(parse_copy_source("b/a/b?versionId=null").unwrap(), ("b".into(), "a/b".into()));
        assert!(parse_copy_source("/b").is_err());
    }

    #[test]
    fn addressing() {
        let dir = std::env::temp_dir().join(format!("objex-route-{}", random_hex(8)));
        let store = Arc::new(crate::storage::local::LocalEngine::open(&dir, false).unwrap());
        let state = AppState::new(store, vec![], "us-east-1".into(), "s3.example.com".into());
        let split = |host: &str, path: &str| {
            let (parts, _) = Request::get(path).header("host", host).body(()).unwrap().into_parts();
            split_path(&state, &parts).unwrap()
        };
        assert_eq!(split("s3.example.com:9000", "/photos/a/b%20c.jpg"), ("photos".into(), "a/b c.jpg".into()));
        assert_eq!(split("photos.s3.example.com", "/a/b.jpg"), ("photos".into(), "a/b.jpg".into()));
        assert_eq!(split("photos.s3.example.com:9000", "/"), ("photos".into(), "".into()));
        assert_eq!(split("my.dotted.bucket.s3.example.com", "//lead"), ("my.dotted.bucket".into(), "/lead".into()));
        assert_eq!(split("127.0.0.1:9000", "/b/k"), ("b".into(), "k".into()));
        assert_eq!(split("127.0.0.1:9000", "/"), ("".into(), "".into()));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn ports() {
        assert_eq!(strip_port("a.example.com:9000"), "a.example.com");
        assert_eq!(strip_port("[::1]:9000"), "[::1]");
        assert_eq!(strip_port("example.com"), "example.com");
    }
}
