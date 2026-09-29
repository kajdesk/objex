//! Service- and bucket-level operations.

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64URL;
use bytes::Bytes;
use http::StatusCode;
use http_body::Body;

use super::{Ctx, Resp, empty, set, write_owner, xml};
use crate::error::{ErrorCode, S3Error, S3Result};
use crate::storage::{BucketUpdate, CorsRule, ListQuery};
use crate::util::{iso8601, uri_encode_path};
use crate::xml::{CorsConfiguration, Delete, XmlWriter, parse};

const ALL_USERS: &str = "http://acs.amazonaws.com/groups/global/AllUsers";

pub async fn list_buckets<B>(ctx: Ctx<B>) -> S3Result<Resp> {
    let mut buckets = ctx.state.store.list_buckets().await?;
    if let Some(k) = &ctx.auth.key {
        buckets.retain(|b| k.can_access(&b.name));
    }
    let mut w = XmlWriter::new("ListAllMyBucketsResult");
    write_owner(&mut w, "Owner");
    w.open("Buckets");
    for b in &buckets {
        w.open("Bucket").elem("Name", &b.name).elem("CreationDate", iso8601(&b.created)).close();
    }
    w.close();
    Ok(xml(w.finish()))
}

/// Parse a canned ACL header into "public read?".
fn canned_acl(v: Option<&str>) -> S3Result<Option<bool>> {
    match v.map(str::trim) {
        None => Ok(None),
        Some("private") | Some("bucket-owner-full-control") | Some("bucket-owner-read") => Ok(Some(false)),
        Some("public-read") | Some("public-read-write") => Ok(Some(true)),
        Some(other) => Err(S3Error::msg(ErrorCode::InvalidArgument, format!("Unsupported canned ACL {other}"))),
    }
}

pub async fn create<B>(mut ctx: Ctx<B>) -> S3Result<Resp>
where
    B: Body<Data = Bytes> + Unpin + Send + 'static,
    B::Error: std::fmt::Display,
{
    let public = canned_acl(ctx.header("x-amz-acl"))?.unwrap_or(false);
    // Any CreateBucketConfiguration (location constraint) is accepted and ignored.
    ctx.read_body().await?;
    ctx.state.store.create_bucket(&ctx.bucket, public).await?;
    let mut r = empty(StatusCode::OK);
    set(r.headers_mut(), "location", format!("/{}", ctx.bucket));
    Ok(r)
}

pub async fn head<B>(ctx: Ctx<B>) -> S3Result<Resp> {
    ctx.state.store.get_bucket(&ctx.bucket).await?;
    let mut r = empty(StatusCode::OK);
    set(r.headers_mut(), "x-amz-bucket-region", &ctx.state.region);
    Ok(r)
}

pub async fn delete<B>(ctx: Ctx<B>) -> S3Result<Resp> {
    ctx.state.store.delete_bucket(&ctx.bucket).await?;
    Ok(empty(StatusCode::NO_CONTENT))
}

pub async fn location<B>(ctx: Ctx<B>) -> S3Result<Resp> {
    ctx.state.store.get_bucket(&ctx.bucket).await?;
    let region = if ctx.state.region == "us-east-1" { "" } else { ctx.state.region.as_str() };
    let mut w = XmlWriter::new("LocationConstraint");
    w.raw(&crate::util::xml_escape(region));
    Ok(xml(w.finish()))
}

pub async fn versioning<B>(ctx: Ctx<B>) -> S3Result<Resp> {
    ctx.state.store.get_bucket(&ctx.bucket).await?;
    Ok(xml(XmlWriter::new("VersioningConfiguration").finish()))
}

pub fn acl_xml(public: bool) -> String {
    let mut w = XmlWriter::new("AccessControlPolicy");
    write_owner(&mut w, "Owner");
    w.open("AccessControlList");
    w.open("Grant")
        .raw(r#"<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser"><ID>objex</ID><DisplayName>objex</DisplayName></Grantee>"#)
        .elem("Permission", "FULL_CONTROL")
        .close();
    if public {
        w.open("Grant")
            .raw(&format!(r#"<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="Group"><URI>{ALL_USERS}</URI></Grantee>"#))
            .elem("Permission", "READ")
            .close();
    }
    w.finish()
}

pub async fn get_acl<B>(ctx: Ctx<B>) -> S3Result<Resp> {
    let info = ctx.state.store.get_bucket(&ctx.bucket).await?;
    Ok(xml(acl_xml(info.public_read)))
}

pub async fn put_acl<B>(mut ctx: Ctx<B>) -> S3Result<Resp>
where
    B: Body<Data = Bytes> + Unpin + Send + 'static,
    B::Error: std::fmt::Display,
{
    let public = match canned_acl(ctx.header("x-amz-acl"))? {
        Some(p) => p,
        None => {
            // An AccessControlPolicy body: public if it grants AllUsers READ.
            let body = ctx.read_body().await?;
            let s = String::from_utf8_lossy(&body);
            s.split("<Grant>").any(|g| g.contains(ALL_USERS) && (g.contains(">READ<") || g.contains(">FULL_CONTROL<")))
        }
    };
    ctx.state.store.update_bucket(&ctx.bucket, BucketUpdate::PublicRead(public)).await?;
    Ok(empty(StatusCode::OK))
}

pub fn cors_xml(rules: &[CorsRule]) -> String {
    let mut w = XmlWriter::new("CORSConfiguration");
    for r in rules {
        w.open("CORSRule");
        w.opt("ID", r.id.as_ref());
        r.allowed_origins.iter().for_each(|v| _ = w.elem("AllowedOrigin", v));
        r.allowed_methods.iter().for_each(|v| _ = w.elem("AllowedMethod", v));
        r.allowed_headers.iter().for_each(|v| _ = w.elem("AllowedHeader", v));
        r.expose_headers.iter().for_each(|v| _ = w.elem("ExposeHeader", v));
        w.opt("MaxAgeSeconds", r.max_age_seconds.map(|s| s.to_string()));
        w.close();
    }
    w.finish()
}

pub async fn get_cors<B>(ctx: Ctx<B>) -> S3Result<Resp> {
    let info = ctx.state.store.get_bucket(&ctx.bucket).await?;
    match info.cors {
        Some(rules) if !rules.is_empty() => Ok(xml(cors_xml(&rules))),
        _ => Err(ErrorCode::NoSuchCORSConfiguration.into()),
    }
}

pub async fn put_cors<B>(mut ctx: Ctx<B>) -> S3Result<Resp>
where
    B: Body<Data = Bytes> + Unpin + Send + 'static,
    B::Error: std::fmt::Display,
{
    let body = ctx.read_body().await?;
    let cfg: CorsConfiguration = parse(&body)?;
    if cfg.rules.is_empty() || cfg.rules.len() > 100 {
        return Err(S3Error::msg(ErrorCode::MalformedXML, "A CORS configuration must have between 1 and 100 rules"));
    }
    let mut rules = Vec::with_capacity(cfg.rules.len());
    for r in cfg.rules {
        if r.allowed_origins.is_empty() || r.allowed_methods.is_empty() {
            return Err(S3Error::msg(ErrorCode::MalformedXML, "Each CORS rule needs an AllowedOrigin and an AllowedMethod"));
        }
        for m in &r.allowed_methods {
            if !["GET", "PUT", "HEAD", "POST", "DELETE"].contains(&m.as_str()) {
                return Err(S3Error::msg(ErrorCode::InvalidRequest, format!("Found unsupported HTTP method in CORS config. Unsupported method is {m}")));
            }
        }
        rules.push(CorsRule::from(r));
    }
    ctx.state.store.update_bucket(&ctx.bucket, BucketUpdate::Cors(Some(rules))).await?;
    Ok(empty(StatusCode::OK))
}

pub async fn delete_cors<B>(ctx: Ctx<B>) -> S3Result<Resp> {
    ctx.state.store.update_bucket(&ctx.bucket, BucketUpdate::Cors(None)).await?;
    Ok(empty(StatusCode::NO_CONTENT))
}

pub async fn delete_objects<B>(mut ctx: Ctx<B>) -> S3Result<Resp>
where
    B: Body<Data = Bytes> + Unpin + Send + 'static,
    B::Error: std::fmt::Display,
{
    let body = ctx.read_body().await?;
    let req: Delete = parse(&body)?;
    if req.objects.len() > 1000 {
        return Err(S3Error::msg(ErrorCode::MalformedXML, "The request must contain no more than 1000 keys"));
    }
    let keys: Vec<String> = req.objects.into_iter().map(|o| o.key).collect();
    let results = ctx.state.store.delete_objects(&ctx.bucket, &keys).await?;
    let mut w = XmlWriter::new("DeleteResult");
    for (k, r) in keys.iter().zip(results) {
        match r {
            Ok(()) if !req.quiet => {
                w.open("Deleted").elem("Key", k).close();
            }
            Ok(()) => {}
            Err(e) => {
                w.open("Error").elem("Key", k).elem("Code", e.code.as_str()).elem("Message", &e.message).close();
            }
        }
    }
    Ok(xml(w.finish()))
}

fn max_keys(v: Option<&str>, name: &str) -> S3Result<usize> {
    match v {
        None => Ok(1000),
        Some(s) => s
            .parse::<i64>()
            .ok()
            .filter(|n| *n >= 0)
            .map(|n| n.min(1000) as usize)
            .ok_or_else(|| S3Error::msg(ErrorCode::InvalidArgument, format!("Provided {name} not an integer or within integer range"))),
    }
}

pub async fn list_objects<B>(ctx: Ctx<B>) -> S3Result<Resp> {
    let v2 = ctx.q("list-type") == Some("2");
    let url = match ctx.q("encoding-type") {
        None => false,
        Some("url") => true,
        Some(_) => return Err(S3Error::msg(ErrorCode::InvalidArgument, "Invalid Encoding Method specified in Request")),
    };
    let enc = |s: &str| if url { uri_encode_path(s) } else { s.to_string() };
    let prefix = ctx.q("prefix").unwrap_or("").to_string();
    let delimiter = ctx.q("delimiter").filter(|d| !d.is_empty()).map(str::to_string);
    let max = max_keys(ctx.q("max-keys"), "max-keys")?;
    let start_after = ctx.q("start-after").unwrap_or("").to_string();
    let token = ctx.q("continuation-token").map(str::to_string);
    let marker = if v2 {
        match &token {
            Some(t) => String::from_utf8(B64URL.decode(t).map_err(|_| S3Error::msg(ErrorCode::InvalidArgument, "The continuation token provided is incorrect"))?)
                .map_err(|_| S3Error::msg(ErrorCode::InvalidArgument, "The continuation token provided is incorrect"))?,
            None => start_after.clone(),
        }
    } else {
        ctx.q("marker").unwrap_or("").to_string()
    };
    let res = ctx
        .state
        .store
        .list_objects(&ctx.bucket, ListQuery { prefix: prefix.clone(), delimiter: delimiter.clone(), marker: marker.clone(), max_keys: max })
        .await?;
    let fetch_owner = !v2 || ctx.q("fetch-owner") == Some("true");

    let mut w = XmlWriter::new("ListBucketResult");
    w.elem("Name", &ctx.bucket).elem("Prefix", enc(&prefix));
    if v2 {
        if !start_after.is_empty() {
            w.elem("StartAfter", enc(&start_after));
        }
        w.opt("ContinuationToken", token.as_ref());
        w.elem("KeyCount", (res.objects.len() + res.prefixes.len()).to_string());
    } else {
        w.elem("Marker", enc(&marker));
    }
    w.elem("MaxKeys", max.to_string());
    w.opt("Delimiter", delimiter.as_deref().map(enc));
    w.elem("IsTruncated", res.truncated.to_string());
    if let Some(next) = &res.next_marker {
        if v2 {
            w.elem("NextContinuationToken", B64URL.encode(next));
        } else {
            w.elem("NextMarker", enc(next));
        }
    }
    if url {
        w.elem("EncodingType", "url");
    }
    for o in &res.objects {
        w.open("Contents")
            .elem("Key", enc(&o.key))
            .elem("LastModified", iso8601(&o.last_modified))
            .elem("ETag", format!("\"{}\"", o.etag))
            .elem("Size", o.size.to_string());
        if let Some(c) = &o.checksum {
            w.elem("ChecksumAlgorithm", c.algo.name()).elem("ChecksumType", crate::checksum::ChecksumType::of(c).name());
        }
        w.elem("StorageClass", "STANDARD");
        if fetch_owner {
            write_owner(&mut w, "Owner");
        }
        w.close();
    }
    for p in &res.prefixes {
        w.open("CommonPrefixes").elem("Prefix", enc(p)).close();
    }
    Ok(xml(w.finish()))
}
