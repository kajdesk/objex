//! Object-level operations.

use std::collections::BTreeMap;

use bytes::Bytes;
use http::{HeaderMap, StatusCode};
use http_body::Body;

use super::{Ctx, Resp, empty, empty_body, set, set_checksum, stream_body, xml};
use crate::error::{ErrorCode, S3Error, S3Result};
use crate::storage::{CopyOptions, ObjectInfo, ObjectMetadata, PutOptions, RangeSpec, ReadConditions, WriteConditions};
use crate::util::{http_date, iso8601, parse_http_date};
use crate::xml::XmlWriter;

/// Limit on the total size of user metadata, as in S3.
const MAX_USER_META: usize = 2048;

/// Standard and user metadata from request headers.
pub fn meta_from_headers(h: &HeaderMap) -> S3Result<ObjectMetadata> {
    let get = |n: &str| h.get(n).and_then(|v| v.to_str().ok()).map(str::to_string);
    let mut m = ObjectMetadata {
        content_type: get("content-type"),
        content_encoding: get("content-encoding"),
        content_disposition: get("content-disposition"),
        content_language: get("content-language"),
        cache_control: get("cache-control"),
        expires: get("expires"),
        user: BTreeMap::new(),
    };
    // aws-chunked is a transfer detail, not a property of the object.
    if let Some(ce) = &m.content_encoding {
        let rest: Vec<&str> = ce.split(',').map(str::trim).filter(|e| !e.is_empty() && !e.eq_ignore_ascii_case("aws-chunked")).collect();
        m.content_encoding = (!rest.is_empty()).then(|| rest.join(","));
    }
    let mut size = 0;
    for (name, value) in h {
        if let Some(n) = name.as_str().strip_prefix("x-amz-meta-") {
            let v = String::from_utf8_lossy(value.as_bytes()).into_owned();
            size += n.len() + v.len();
            m.user.entry(n.to_string()).and_modify(|e: &mut String| e.push_str(&format!(",{v}"))).or_insert(v);
        }
    }
    if size > MAX_USER_META {
        return Err(ErrorCode::MetadataTooLarge.into());
    }
    Ok(m)
}

fn header_opt(h: &HeaderMap, n: &str) -> Option<String> {
    h.get(n).and_then(|v| v.to_str().ok()).map(str::to_string)
}

fn read_conditions(h: &HeaderMap, prefix: &str) -> S3Result<ReadConditions> {
    let date = |n: &str| h.get(format!("{prefix}{n}")).and_then(|v| v.to_str().ok()).and_then(parse_http_date);
    Ok(ReadConditions {
        if_match: header_opt(h, &format!("{prefix}if-match")),
        if_none_match: header_opt(h, &format!("{prefix}if-none-match")),
        if_modified_since: date("if-modified-since"),
        if_unmodified_since: date("if-unmodified-since"),
    })
}

pub fn write_conditions(h: &HeaderMap) -> WriteConditions {
    WriteConditions { if_match: header_opt(h, "if-match"), if_none_match: header_opt(h, "if-none-match") }
}

/// Headers describing an object: metadata, ETag, dates.
fn object_headers<B>(ctx: &Ctx<B>, info: &ObjectInfo, h: &mut HeaderMap) {
    let m = &info.meta;
    let over = |n: &str| ctx.q(n).map(str::to_string);
    set(h, "content-type", over("response-content-type").or(m.content_type.clone()).unwrap_or_else(|| "application/octet-stream".into()));
    for (header, query, value) in [
        ("content-encoding", "response-content-encoding", &m.content_encoding),
        ("content-disposition", "response-content-disposition", &m.content_disposition),
        ("content-language", "response-content-language", &m.content_language),
        ("cache-control", "response-cache-control", &m.cache_control),
        ("expires", "response-expires", &m.expires),
    ] {
        if let Some(v) = over(query).or(value.clone()) {
            set(h, header, v);
        }
    }
    for (k, v) in &m.user {
        if let (Ok(n), Ok(v)) = (http::HeaderName::try_from(format!("x-amz-meta-{k}")), http::HeaderValue::from_str(v)) {
            h.insert(n, v);
        }
    }
    set(h, "etag", format!("\"{}\"", info.etag));
    set(h, "last-modified", http_date(&info.last_modified));
    set(h, "accept-ranges", "bytes");
    if info.parts_count > 0 {
        set(h, "x-amz-mp-parts-count", info.parts_count.to_string());
    }
    set(h, "x-amz-storage-class", "STANDARD");
}

fn not_modified(info: &ObjectInfo) -> Resp {
    let mut r = empty(StatusCode::NOT_MODIFIED);
    set(r.headers_mut(), "etag", format!("\"{}\"", info.etag));
    set(r.headers_mut(), "last-modified", http_date(&info.last_modified));
    r
}

/// GetObject and HeadObject.
pub async fn get<B>(ctx: Ctx<B>, head: bool) -> S3Result<Resp> {
    let cond = read_conditions(&ctx.parts.headers, "")?;
    let part_number = match ctx.q("partNumber") {
        None => None,
        Some(s) => Some(s.parse::<u32>().ok().filter(|n| (1..=10_000).contains(n)).ok_or_else(|| S3Error::msg(ErrorCode::InvalidArgument, "Part number must be an integer between 1 and 10000, inclusive"))?),
    };
    let range = ctx.header("range").and_then(RangeSpec::parse);
    if part_number.is_some() && ctx.header("range").is_some() {
        return Err(S3Error::msg(ErrorCode::InvalidRequest, "Cannot specify both Range header and partNumber query parameter"));
    }

    // HEAD needs the reader only for part sizes; creating one opens no files.
    let (info, reader) = if head && part_number.is_none() {
        (ctx.state.store.head_object(&ctx.bucket, &ctx.key).await?, None)
    } else {
        let (i, r) = ctx.state.store.get_object(&ctx.bucket, &ctx.key).await?;
        (i, Some(r))
    };
    match cond.check(&info, false) {
        Err(e) if e.code == ErrorCode::NotModified => return Ok(not_modified(&info)),
        r => r?,
    }

    let size = info.size;
    // (start, len, whether this is a partial response)
    let (start, len, partial) = match (part_number, range) {
        (Some(n), _) if info.parts_count == 0 => {
            if n != 1 {
                return Err(ErrorCode::InvalidPartNumber.into());
            }
            (0, size, false)
        }
        (Some(n), _) => {
            let (start, len) = reader.as_ref().and_then(|r| r.part_range(n)).ok_or(ErrorCode::InvalidPartNumber)?;
            (start, len, true)
        }
        // No range is satisfiable on an empty object; resolve() reports that.
        (None, Some(r)) => match r.resolve(size) {
            Ok((s, l)) => (s, l, true),
            Err(e) => {
                let mut resp = super::error_response(&e, ctx.parts.uri.path(), "", head);
                set(resp.headers_mut(), "content-range", format!("bytes */{size}"));
                return Ok(resp);
            }
        },
        _ => (0, size, false),
    };

    let body = match reader {
        Some(r) if len > 0 && !head => stream_body(r.open(start, len).await.map_err(S3Error::internal)?),
        _ => empty_body(),
    };
    let mut resp = http::Response::new(body);
    let h = resp.headers_mut();
    object_headers(&ctx, &info, h);
    set(h, "content-length", len.to_string());
    if partial {
        *resp.status_mut() = StatusCode::PARTIAL_CONTENT;
        set(resp.headers_mut(), "content-range", format!("bytes {}-{}/{}", start, start + len - 1, size));
    }
    let want_checksum = ctx.header("x-amz-checksum-mode").is_some_and(|m| m.eq_ignore_ascii_case("ENABLED"));
    if want_checksum && !partial {
        set_checksum(resp.headers_mut(), info.checksum.as_ref());
    }
    Ok(resp)
}

pub async fn put<B>(mut ctx: Ctx<B>) -> S3Result<Resp>
where
    B: Body<Data = Bytes> + Unpin + Send + 'static,
    B::Error: std::fmt::Display,
{
    let expect = ctx.put_expect()?;
    if expect.size.is_none() && ctx.header("transfer-encoding").is_none() {
        return Err(ErrorCode::MissingContentLength.into());
    }
    let meta = meta_from_headers(&ctx.parts.headers)?;
    let cond = write_conditions(&ctx.parts.headers);
    let mut src = ctx.source();
    let info = ctx.state.store.put_object(&ctx.bucket, &ctx.key, src.as_mut(), PutOptions { meta, expect, cond }).await?;
    let mut r = empty(StatusCode::OK);
    set(r.headers_mut(), "etag", format!("\"{}\"", info.etag));
    set_checksum(r.headers_mut(), info.checksum.as_ref());
    Ok(r)
}

pub async fn copy<B>(ctx: Ctx<B>, src_bucket: String, src_key: String) -> S3Result<Resp> {
    let replace = match ctx.header("x-amz-metadata-directive").map(|d| d.to_ascii_uppercase()) {
        None => false,
        Some(d) if d == "COPY" => false,
        Some(d) if d == "REPLACE" => true,
        Some(_) => return Err(S3Error::msg(ErrorCode::InvalidArgument, "Unknown metadata directive.")),
    };
    let opts = CopyOptions {
        replace_meta: if replace { Some(meta_from_headers(&ctx.parts.headers)?) } else { None },
        src_cond: read_conditions(&ctx.parts.headers, "x-amz-copy-source-")?,
        dst_cond: write_conditions(&ctx.parts.headers),
    };
    let info = ctx.state.store.copy_object(&src_bucket, &src_key, &ctx.bucket, &ctx.key, opts).await?;
    let mut w = XmlWriter::new("CopyObjectResult");
    w.elem("LastModified", iso8601(&info.last_modified)).elem("ETag", format!("\"{}\"", info.etag)).checksum(info.checksum.as_ref());
    if let Some(c) = &info.checksum {
        w.elem("ChecksumType", crate::checksum::ChecksumType::of(c).name());
    }
    Ok(xml(w.finish()))
}

pub async fn delete<B>(ctx: Ctx<B>) -> S3Result<Resp> {
    // S3 does not reveal whether the key existed.
    ctx.state.store.delete_object(&ctx.bucket, &ctx.key).await?;
    Ok(empty(StatusCode::NO_CONTENT))
}

pub async fn get_acl<B>(ctx: Ctx<B>) -> S3Result<Resp> {
    let bucket = ctx.state.store.get_bucket(&ctx.bucket).await?;
    ctx.state.store.head_object(&ctx.bucket, &ctx.key).await?;
    Ok(xml(super::bucket::acl_xml(bucket.public_read)))
}

/// Object ACLs are not stored (access is per bucket); accept only "private".
pub async fn put_acl<B>(ctx: Ctx<B>) -> S3Result<Resp> {
    ctx.state.store.head_object(&ctx.bucket, &ctx.key).await?;
    match ctx.header("x-amz-acl") {
        None | Some("private") | Some("bucket-owner-full-control") => Ok(empty(StatusCode::OK)),
        Some(_) => Err(S3Error::msg(ErrorCode::NotImplemented, "Per-object ACLs are not supported; use a public-read bucket")),
    }
}

pub async fn get_tagging<B>(ctx: Ctx<B>) -> S3Result<Resp> {
    ctx.state.store.head_object(&ctx.bucket, &ctx.key).await?;
    let mut w = XmlWriter::new("Tagging");
    w.open("TagSet").close();
    Ok(xml(w.finish()))
}
