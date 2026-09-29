//! Multipart upload operations.

use bytes::Bytes;
use http::StatusCode;
use http_body::Body;

use super::object::{meta_from_headers, write_conditions};
use super::{Ctx, Resp, empty, set, write_owner, xml};
use crate::checksum::{Checksum, ChecksumAlgo, ChecksumType};
use crate::error::{ErrorCode, S3Error, S3Result};
use crate::storage::{CompleteOptions, CompletePart, ListUploadsQuery, RangeSpec};
use crate::util::{iso8601, uri_encode_path};
use crate::xml::{CompleteMultipartUpload, XmlWriter, parse};

fn upload_id<B>(ctx: &Ctx<B>) -> S3Result<String> {
    ctx.q("uploadId").filter(|u| !u.is_empty()).map(str::to_string).ok_or_else(|| ErrorCode::NoSuchUpload.into())
}

fn part_number<B>(ctx: &Ctx<B>) -> S3Result<u32> {
    ctx.q("partNumber")
        .and_then(|p| p.parse().ok())
        .filter(|n| (1..=crate::storage::MAX_PART_NUMBER).contains(n))
        .ok_or_else(|| S3Error::msg(ErrorCode::InvalidArgument, "Part number must be an integer between 1 and 10000, inclusive"))
}

pub async fn create<B>(ctx: Ctx<B>) -> S3Result<Resp> {
    let meta = meta_from_headers(&ctx.parts.headers)?;
    let algo = match ctx.header("x-amz-checksum-algorithm") {
        None => None,
        Some(a) => Some(ChecksumAlgo::parse(a).ok_or_else(|| S3Error::msg(ErrorCode::InvalidRequest, "Invalid checksum algorithm"))?),
    };
    let ctype = match ctx.header("x-amz-checksum-type") {
        None => None,
        Some(t) => Some(ChecksumType::parse(t).ok_or_else(|| S3Error::msg(ErrorCode::InvalidRequest, "Invalid checksum type"))?),
    };
    let checksum = match (algo, ctype) {
        (Some(a), t) => Some((a, t.unwrap_or(a.default_type()))),
        (None, Some(_)) => return Err(S3Error::msg(ErrorCode::InvalidRequest, "The x-amz-checksum-type header requires x-amz-checksum-algorithm")),
        (None, None) => None,
    };
    let id = ctx.state.store.create_multipart(&ctx.bucket, &ctx.key, meta, checksum).await?;
    let mut w = XmlWriter::new("InitiateMultipartUploadResult");
    w.elem("Bucket", &ctx.bucket).elem("Key", &ctx.key).elem("UploadId", &id);
    let mut r = xml(w.finish());
    if let Some((a, t)) = checksum {
        set(r.headers_mut(), "x-amz-checksum-algorithm", a.name());
        set(r.headers_mut(), "x-amz-checksum-type", t.name());
    }
    Ok(r)
}

pub async fn upload_part<B>(mut ctx: Ctx<B>) -> S3Result<Resp>
where
    B: Body<Data = Bytes> + Unpin + Send + 'static,
    B::Error: std::fmt::Display,
{
    let (id, n) = (upload_id(&ctx)?, part_number(&ctx)?);
    let expect = ctx.put_expect()?;
    if expect.size.is_none() && ctx.header("transfer-encoding").is_none() {
        return Err(ErrorCode::MissingContentLength.into());
    }
    let mut src = ctx.source();
    let part = ctx.state.store.upload_part(&ctx.bucket, &ctx.key, &id, n, src.as_mut(), expect).await?;
    let mut r = empty(StatusCode::OK);
    set(r.headers_mut(), "etag", format!("\"{}\"", part.etag));
    if let Some(c) = &part.checksum {
        set(r.headers_mut(), c.algo.header(), &c.value);
    }
    Ok(r)
}

pub async fn upload_part_copy<B>(ctx: Ctx<B>, src_bucket: String, src_key: String) -> S3Result<Resp> {
    let (id, n) = (upload_id(&ctx)?, part_number(&ctx)?);
    let range = match ctx.header("x-amz-copy-source-range") {
        None => None,
        Some(r) => match RangeSpec::parse(r) {
            Some(RangeSpec::FromTo(a, Some(b))) => Some((a, b)),
            _ => return Err(S3Error::msg(ErrorCode::InvalidArgument, "The x-amz-copy-source-range value must be of the form bytes=first-last where first and last are the zero-based offsets of the first and last bytes to copy")),
        },
    };
    let h = &ctx.parts.headers;
    let date = |n: &str| h.get(n).and_then(|v| v.to_str().ok()).and_then(crate::util::parse_http_date);
    let hdr = |n: &str| h.get(n).and_then(|v| v.to_str().ok()).map(str::to_string);
    let cond = crate::storage::ReadConditions {
        if_match: hdr("x-amz-copy-source-if-match"),
        if_none_match: hdr("x-amz-copy-source-if-none-match"),
        if_modified_since: date("x-amz-copy-source-if-modified-since"),
        if_unmodified_since: date("x-amz-copy-source-if-unmodified-since"),
    };
    let (part, _src) = ctx.state.store.upload_part_copy(&src_bucket, &src_key, range, cond, &ctx.bucket, &ctx.key, &id, n).await?;
    let mut w = XmlWriter::new("CopyPartResult");
    w.elem("LastModified", iso8601(&part.last_modified)).elem("ETag", format!("\"{}\"", part.etag)).checksum(part.checksum.as_ref());
    Ok(xml(w.finish()))
}

pub async fn complete<B>(mut ctx: Ctx<B>) -> S3Result<Resp>
where
    B: Body<Data = Bytes> + Unpin + Send + 'static,
    B::Error: std::fmt::Display,
{
    let id = upload_id(&ctx)?;
    let body = ctx.read_body().await?;
    let req: CompleteMultipartUpload = parse(&body)?;
    let parts = req.parts.iter().map(|p| CompletePart { number: p.number, etag: p.etag.trim().trim_matches('"').to_string(), checksum: p.checksum() }).collect();
    let checksum = ChecksumAlgo::ALL.into_iter().find_map(|a| ctx.header(a.header()).map(|v| Checksum { algo: a, value: v.trim().to_string() }));
    let opts = CompleteOptions { cond: write_conditions(&ctx.parts.headers), checksum };
    let info = ctx.state.store.complete_multipart(&ctx.bucket, &ctx.key, &id, parts, opts).await?;

    let mut w = XmlWriter::new("CompleteMultipartUploadResult");
    w.elem("Location", format!("http://{}/{}/{}", ctx.host(), ctx.bucket, uri_encode_path(&ctx.key)))
        .elem("Bucket", &ctx.bucket)
        .elem("Key", &ctx.key)
        .elem("ETag", format!("\"{}\"", info.etag))
        .checksum(info.checksum.as_ref());
    if let Some(c) = &info.checksum {
        w.elem("ChecksumType", ChecksumType::of(c).name());
    }
    Ok(xml(w.finish()))
}

pub async fn abort<B>(ctx: Ctx<B>) -> S3Result<Resp> {
    let id = upload_id(&ctx)?;
    ctx.state.store.abort_multipart(&ctx.bucket, &ctx.key, &id).await?;
    Ok(empty(StatusCode::NO_CONTENT))
}

pub async fn list_parts<B>(ctx: Ctx<B>) -> S3Result<Resp> {
    let id = upload_id(&ctx)?;
    let marker: u32 = match ctx.q("part-number-marker") {
        None | Some("") => 0,
        Some(m) => m.parse().map_err(|_| S3Error::msg(ErrorCode::InvalidArgument, "Invalid part-number-marker"))?,
    };
    let max: usize = match ctx.q("max-parts") {
        None => 1000,
        Some(m) => m.parse::<usize>().map_err(|_| S3Error::msg(ErrorCode::InvalidArgument, "Invalid max-parts"))?.min(1000),
    };
    let res = ctx.state.store.list_parts(&ctx.bucket, &ctx.key, &id, marker, max).await?;
    let mut w = XmlWriter::new("ListPartsResult");
    w.elem("Bucket", &ctx.bucket).elem("Key", &ctx.key).elem("UploadId", &id);
    write_owner(&mut w, "Initiator");
    write_owner(&mut w, "Owner");
    w.elem("StorageClass", "STANDARD")
        .elem("PartNumberMarker", marker.to_string())
        .elem("NextPartNumberMarker", res.next_marker.to_string())
        .elem("MaxParts", max.to_string())
        .elem("IsTruncated", res.truncated.to_string());
    if let Some(a) = res.upload.checksum_algo {
        w.elem("ChecksumAlgorithm", a.name()).elem("ChecksumType", res.upload.checksum_type.unwrap_or(a.default_type()).name());
    }
    for p in &res.parts {
        w.open("Part")
            .elem("PartNumber", p.number.to_string())
            .elem("LastModified", iso8601(&p.last_modified))
            .elem("ETag", format!("\"{}\"", p.etag))
            .elem("Size", p.size.to_string())
            .checksum(p.checksum.as_ref())
            .close();
    }
    Ok(xml(w.finish()))
}

pub async fn list_uploads<B>(ctx: Ctx<B>) -> S3Result<Resp> {
    let url = ctx.q("encoding-type") == Some("url");
    let enc = |s: &str| if url { uri_encode_path(s) } else { s.to_string() };
    let max: usize = match ctx.q("max-uploads") {
        None => 1000,
        Some(m) => m.parse::<usize>().map_err(|_| S3Error::msg(ErrorCode::InvalidArgument, "Invalid max-uploads"))?.min(1000),
    };
    let q = ListUploadsQuery {
        prefix: ctx.q("prefix").unwrap_or("").to_string(),
        delimiter: ctx.q("delimiter").filter(|d| !d.is_empty()).map(str::to_string),
        key_marker: ctx.q("key-marker").unwrap_or("").to_string(),
        upload_id_marker: ctx.q("upload-id-marker").unwrap_or("").to_string(),
        max_uploads: max,
    };
    let res = ctx.state.store.list_uploads(&ctx.bucket, q.clone()).await?;
    let mut w = XmlWriter::new("ListMultipartUploadsResult");
    w.elem("Bucket", &ctx.bucket)
        .elem("KeyMarker", enc(&q.key_marker))
        .elem("UploadIdMarker", &q.upload_id_marker)
        .elem("NextKeyMarker", enc(&res.next_key_marker))
        .elem("NextUploadIdMarker", &res.next_upload_id_marker)
        .elem("MaxUploads", max.to_string())
        .elem("IsTruncated", res.truncated.to_string())
        .elem("Prefix", enc(&q.prefix));
    w.opt("Delimiter", q.delimiter.as_deref().map(enc));
    if url {
        w.elem("EncodingType", "url");
    }
    for u in &res.uploads {
        w.open("Upload").elem("Key", enc(&u.key)).elem("UploadId", &u.upload_id);
        write_owner(&mut w, "Initiator");
        write_owner(&mut w, "Owner");
        w.elem("StorageClass", "STANDARD").elem("Initiated", iso8601(&u.initiated));
        if let Some(a) = u.checksum_algo {
            w.elem("ChecksumAlgorithm", a.name()).elem("ChecksumType", u.checksum_type.unwrap_or(a.default_type()).name());
        }
        w.close();
    }
    for p in &res.prefixes {
        w.open("CommonPrefixes").elem("Prefix", enc(p)).close();
    }
    Ok(xml(w.finish()))
}
