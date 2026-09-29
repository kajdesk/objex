//! AWS Signature Version 4: header auth, presigned URLs, and streaming chunk signatures.

use chrono::{DateTime, Duration, Utc};
use hmac::{Hmac, KeyInit, Mac};
use http::HeaderMap;
use http::request::Parts;
use sha2::{Digest, Sha256};

use crate::config::KeyConfig;
use crate::error::{ErrorCode, S3Error, S3Result};
use crate::util::{ct_eq, parse_amz_date, parse_http_date, uri_decode, uri_encode, uri_encode_path};

pub const ALGORITHM: &str = "AWS4-HMAC-SHA256";
pub const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";
pub const STREAMING_SIGNED: &str = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";
pub const STREAMING_SIGNED_TRAILER: &str = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER";
pub const STREAMING_UNSIGNED_TRAILER: &str = "STREAMING-UNSIGNED-PAYLOAD-TRAILER";
pub const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

const MAX_SKEW_MINUTES: i64 = 15;
const MAX_PRESIGN_SECONDS: i64 = 7 * 24 * 3600;

type HmacSha256 = Hmac<Sha256>;

fn hmac(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut m = HmacSha256::new_from_slice(key).expect("hmac accepts any key length");
    m.update(data);
    m.finalize().into_bytes().into()
}

pub fn sha256_hex(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

fn signing_key(secret: &str, date: &str, region: &str, service: &str) -> [u8; 32] {
    let k = hmac(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let k = hmac(&k, region.as_bytes());
    let k = hmac(&k, service.as_bytes());
    hmac(&k, b"aws4_request")
}

/// How the request body is protected.
#[derive(Debug, Clone)]
pub enum Payload {
    /// Not signed (UNSIGNED-PAYLOAD, presigned URLs, anonymous requests).
    Unsigned,
    /// The body must hash to this SHA-256.
    Sha256([u8; 32]),
    /// aws-chunked encoding. `signer` is set when each chunk carries a signature.
    Streaming { signer: Option<ChunkSigner>, trailer: bool },
}

/// Result of authenticating a request.
#[derive(Debug, Clone)]
pub struct Auth {
    /// None for anonymous requests.
    pub key: Option<KeyConfig>,
    pub payload: Payload,
}

/// Verifies the chained signatures of aws-chunked uploads.
#[derive(Debug, Clone)]
pub struct ChunkSigner {
    key: [u8; 32],
    amz_date: String,
    scope: String,
    prev: String,
}

impl ChunkSigner {
    /// Check the signature of the next chunk, given the SHA-256 of its data.
    pub fn verify_chunk(&mut self, chunk_sha256: &[u8], signature: &str) -> S3Result<()> {
        let sts = format!(
            "AWS4-HMAC-SHA256-PAYLOAD\n{}\n{}\n{}\n{}\n{}",
            self.amz_date,
            self.scope,
            self.prev,
            EMPTY_SHA256,
            hex::encode(chunk_sha256)
        );
        self.advance(&sts, signature)
    }

    /// Check the signature over the trailing headers (canonical "name:value\n" lines).
    pub fn verify_trailer(&mut self, canonical_trailers: &str, signature: &str) -> S3Result<()> {
        let sts = format!(
            "AWS4-HMAC-SHA256-TRAILER\n{}\n{}\n{}\n{}",
            self.amz_date,
            self.scope,
            self.prev,
            sha256_hex(canonical_trailers.as_bytes())
        );
        self.advance(&sts, signature)
    }

    fn advance(&mut self, string_to_sign: &str, signature: &str) -> S3Result<()> {
        let expected = hex::encode(hmac(&self.key, string_to_sign.as_bytes()));
        if !ct_eq(expected.as_bytes(), signature.as_bytes()) {
            return Err(ErrorCode::SignatureDoesNotMatch.into());
        }
        self.prev = expected;
        Ok(())
    }
}

struct Credential<'a> {
    access_key: &'a str,
    date: &'a str,
    region: &'a str,
    service: &'a str,
}

fn parse_credential(s: &str) -> Option<Credential<'_>> {
    // AKID/20130524/us-east-1/s3/aws4_request (access keys cannot contain '/')
    let mut it = s.rsplitn(5, '/');
    let terminator = it.next()?;
    let service = it.next()?;
    let region = it.next()?;
    let date = it.next()?;
    let access_key = it.next()?;
    (terminator == "aws4_request" && date.len() == 8 && !access_key.is_empty()).then_some(Credential { access_key, date, region, service })
}

/// Parsed query string, percent-decoded.
pub fn parse_query(q: Option<&str>) -> Vec<(String, String)> {
    let Some(q) = q else { return Vec::new() };
    q.split('&')
        .filter(|p| !p.is_empty())
        .map(|p| {
            let (k, v) = p.split_once('=').unwrap_or((p, ""));
            (uri_decode(k).unwrap_or_else(|| k.to_string()), uri_decode(v).unwrap_or_else(|| v.to_string()))
        })
        .collect()
}

fn canonical_query(query: &[(String, String)], skip_signature: bool) -> String {
    let mut pairs: Vec<(String, String)> = query
        .iter()
        .filter(|(k, _)| !(skip_signature && k == "X-Amz-Signature"))
        .map(|(k, v)| (uri_encode(k), uri_encode(v)))
        .collect();
    pairs.sort();
    pairs.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join("&")
}

fn header_value(parts: &Parts, name: &str) -> Option<String> {
    if name == "host"
        && parts.headers.get("host").is_none()
        && let Some(a) = parts.uri.authority()
    {
        return Some(a.as_str().to_string());
    }
    let vals: Vec<String> = parts
        .headers
        .get_all(name)
        .iter()
        .map(|v| {
            let s = String::from_utf8_lossy(v.as_bytes());
            s.split_whitespace().collect::<Vec<_>>().join(" ")
        })
        .collect();
    (!vals.is_empty()).then(|| vals.join(","))
}

fn canonical_headers(parts: &Parts, signed: &[&str]) -> S3Result<String> {
    let mut out = String::new();
    for name in signed {
        let v = header_value(parts, name).ok_or_else(|| S3Error::msg(ErrorCode::AccessDenied, format!("Signed header {name} is missing")))?;
        out.push_str(name);
        out.push(':');
        out.push_str(&v);
        out.push('\n');
    }
    Ok(out)
}

fn header_str<'a>(h: &'a HeaderMap, name: &str) -> Option<&'a str> {
    h.get(name).and_then(|v| v.to_str().ok())
}

/// Canonical URIs to try: the path re-encoded from its decoded form (what AWS SDKs
/// sign), and the path exactly as received (for clients that encode differently).
fn canonical_uris(parts: &Parts) -> Vec<String> {
    let raw = parts.uri.path();
    let raw = if raw.is_empty() { "/" } else { raw };
    let mut out = Vec::with_capacity(2);
    if let Some(decoded) = uri_decode(raw) {
        out.push(uri_encode_path(&decoded));
    }
    if !out.iter().any(|u| u == raw) {
        out.push(raw.to_string());
    }
    out
}

struct Signed<'a> {
    cred: Credential<'a>,
    signed_headers: Vec<&'a str>,
    signature: &'a str,
    amz_date: String,
    payload_hash: String,
    presigned: bool,
}

/// Authenticate a request against the configured keys.
pub fn authenticate(parts: &Parts, query: &[(String, String)], keys: &[KeyConfig], now: DateTime<Utc>) -> S3Result<Auth> {
    let q = |name: &str| query.iter().find(|(k, _)| k == name).map(|(_, v)| v.as_str());

    if let Some(auth) = header_str(&parts.headers, "authorization") {
        let rest = auth
            .strip_prefix(ALGORITHM)
            .ok_or_else(|| S3Error::msg(ErrorCode::InvalidRequest, "Unsupported authorization type. Please use AWS4-HMAC-SHA256."))?;
        let mut cred = None;
        let mut signed_headers = None;
        let mut signature = None;
        for field in rest.split(',') {
            let (k, v) = field.trim().split_once('=').ok_or(ErrorCode::AuthorizationHeaderMalformed)?;
            match k.trim() {
                "Credential" => cred = Some(v.trim()),
                "SignedHeaders" => signed_headers = Some(v.trim()),
                "Signature" => signature = Some(v.trim()),
                _ => {}
            }
        }
        let (Some(cred), Some(sh), Some(signature)) = (cred, signed_headers, signature) else {
            return Err(ErrorCode::AuthorizationHeaderMalformed.into());
        };
        let cred = parse_credential(cred).ok_or(ErrorCode::AuthorizationHeaderMalformed)?;
        let amz_date = match header_str(&parts.headers, "x-amz-date") {
            Some(d) => d.to_string(),
            None => {
                let d = header_str(&parts.headers, "date").and_then(parse_http_date).ok_or_else(|| {
                    S3Error::msg(ErrorCode::AccessDenied, "AWS authentication requires a valid Date or x-amz-date header")
                })?;
                d.format("%Y%m%dT%H%M%SZ").to_string()
            }
        };
        let t = parse_amz_date(&amz_date).ok_or_else(|| S3Error::msg(ErrorCode::AccessDenied, "Invalid x-amz-date"))?;
        if (now - t).num_minutes().abs() >= MAX_SKEW_MINUTES {
            return Err(ErrorCode::RequestTimeTooSkewed.into());
        }
        let payload_hash = header_str(&parts.headers, "x-amz-content-sha256").unwrap_or(UNSIGNED_PAYLOAD).to_string();
        let s = Signed { cred, signed_headers: sh.split(';').collect(), signature, amz_date, payload_hash, presigned: false };
        return verify(parts, query, keys, s);
    }

    if let Some(algo) = q("X-Amz-Algorithm") {
        if algo != ALGORITHM {
            return Err(S3Error::msg(ErrorCode::AuthorizationQueryParametersError, "X-Amz-Algorithm only supports \"AWS4-HMAC-SHA256\""));
        }
        let missing = || S3Error::msg(ErrorCode::AuthorizationQueryParametersError, "Query-string authentication version 4 requires the X-Amz-Algorithm, X-Amz-Credential, X-Amz-Signature, X-Amz-Date, X-Amz-SignedHeaders, and X-Amz-Expires parameters.");
        let cred = q("X-Amz-Credential").ok_or_else(missing)?;
        let amz_date = q("X-Amz-Date").ok_or_else(missing)?;
        let expires = q("X-Amz-Expires").ok_or_else(missing)?;
        let sh = q("X-Amz-SignedHeaders").ok_or_else(missing)?;
        let signature = q("X-Amz-Signature").ok_or_else(missing)?;
        let cred = parse_credential(cred).ok_or(ErrorCode::AuthorizationQueryParametersError)?;
        let t = parse_amz_date(amz_date).ok_or_else(|| S3Error::msg(ErrorCode::AuthorizationQueryParametersError, "Invalid X-Amz-Date"))?;
        let expires: i64 = expires.parse().map_err(|_| S3Error::msg(ErrorCode::AuthorizationQueryParametersError, "X-Amz-Expires should be a number"))?;
        if !(0..=MAX_PRESIGN_SECONDS).contains(&expires) {
            return Err(S3Error::msg(ErrorCode::AuthorizationQueryParametersError, "X-Amz-Expires must be less than a week (in seconds) that is 604800"));
        }
        if t - now > Duration::minutes(MAX_SKEW_MINUTES) {
            return Err(S3Error::msg(ErrorCode::AccessDenied, "Request is not valid yet"));
        }
        if now > t + Duration::seconds(expires) {
            return Err(S3Error::msg(ErrorCode::AccessDenied, "Request has expired"));
        }
        let payload_hash = q("X-Amz-Content-Sha256").unwrap_or(UNSIGNED_PAYLOAD).to_string();
        let s = Signed { cred, signed_headers: sh.split(';').collect(), signature, amz_date: amz_date.to_string(), payload_hash, presigned: true };
        return verify(parts, query, keys, s);
    }

    if q("AWSAccessKeyId").is_some() {
        return Err(S3Error::msg(ErrorCode::InvalidRequest, "Signature Version 2 is not supported. Please use AWS4-HMAC-SHA256."));
    }
    Ok(Auth { key: None, payload: Payload::Unsigned })
}

fn verify(parts: &Parts, query: &[(String, String)], keys: &[KeyConfig], s: Signed<'_>) -> S3Result<Auth> {
    let key = keys.iter().find(|k| k.access_key == s.cred.access_key).ok_or(ErrorCode::InvalidAccessKeyId)?;
    if !s.amz_date.starts_with(s.cred.date) {
        return Err(S3Error::msg(
            if s.presigned { ErrorCode::AuthorizationQueryParametersError } else { ErrorCode::AuthorizationHeaderMalformed },
            "Credential date does not match the request date",
        ));
    }
    if s.cred.service != "s3" {
        return Err(S3Error::msg(ErrorCode::AuthorizationHeaderMalformed, format!("Unsupported service {}", s.cred.service)));
    }
    if !s.signed_headers.contains(&"host") {
        return Err(S3Error::msg(ErrorCode::AccessDenied, "Host must be a signed header"));
    }
    let skey = signing_key(&key.secret_key, s.cred.date, s.cred.region, s.cred.service);
    let scope = format!("{}/{}/{}/aws4_request", s.cred.date, s.cred.region, s.cred.service);
    let headers = canonical_headers(parts, &s.signed_headers)?;
    let cquery = canonical_query(query, s.presigned);
    let matched = canonical_uris(parts).into_iter().any(|uri| {
        let creq = format!("{}\n{}\n{}\n{}\n{}\n{}", parts.method.as_str(), uri, cquery, headers, s.signed_headers.join(";"), s.payload_hash);
        let sts = format!("{ALGORITHM}\n{}\n{}\n{}", s.amz_date, scope, sha256_hex(creq.as_bytes()));
        let expected = hex::encode(hmac(&skey, sts.as_bytes()));
        ct_eq(expected.as_bytes(), s.signature.as_bytes())
    });
    if !matched {
        return Err(ErrorCode::SignatureDoesNotMatch.into());
    }

    let payload = match s.payload_hash.as_str() {
        UNSIGNED_PAYLOAD => Payload::Unsigned,
        STREAMING_UNSIGNED_TRAILER => Payload::Streaming { signer: None, trailer: true },
        STREAMING_SIGNED | STREAMING_SIGNED_TRAILER => Payload::Streaming {
            signer: Some(ChunkSigner { key: skey, amz_date: s.amz_date.clone(), scope, prev: s.signature.to_string() }),
            trailer: s.payload_hash == STREAMING_SIGNED_TRAILER,
        },
        h => {
            let mut b = [0u8; 32];
            hex::decode_to_slice(h, &mut b).map_err(|_| S3Error::msg(ErrorCode::InvalidArgument, "x-amz-content-sha256 must be UNSIGNED-PAYLOAD, a streaming mode, or a valid sha256 value."))?;
            Payload::Sha256(b)
        }
    };
    Ok(Auth { key: Some(key.clone()), payload })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> KeyConfig {
        KeyConfig {
            name: "t".into(),
            access_key: "AKIAIOSFODNN7EXAMPLE".into(),
            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into(),
            buckets: vec![],
            read_only: false,
        }
    }

    fn now() -> DateTime<Utc> {
        parse_amz_date("20130524T000000Z").unwrap()
    }

    // Examples from the AWS S3 SigV4 documentation.
    #[test]
    fn header_get_object() {
        let req = http::Request::get("/test.txt")
            .header("host", "examplebucket.s3.amazonaws.com")
            .header("range", "bytes=0-9")
            .header("x-amz-content-sha256", EMPTY_SHA256)
            .header("x-amz-date", "20130524T000000Z")
            .header(
                "authorization",
                "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;range;x-amz-content-sha256;x-amz-date,Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41",
            )
            .body(())
            .unwrap();
        let (parts, _) = req.into_parts();
        let a = authenticate(&parts, &[], &[key()], now()).unwrap();
        assert!(matches!(a.payload, Payload::Sha256(_)));
    }

    #[test]
    fn presigned_get() {
        let uri = "/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404";
        let req = http::Request::get(uri).header("host", "examplebucket.s3.amazonaws.com").body(()).unwrap();
        let (parts, _) = req.into_parts();
        let q = parse_query(parts.uri.query());
        authenticate(&parts, &q, &[key()], now()).unwrap();
        let late = now() + Duration::seconds(86401);
        assert_eq!(authenticate(&parts, &q, &[key()], late).unwrap_err().code, ErrorCode::AccessDenied);
    }

    #[test]
    fn bad_signature() {
        let req = http::Request::get("/test.txt")
            .header("host", "examplebucket.s3.amazonaws.com")
            .header("x-amz-content-sha256", EMPTY_SHA256)
            .header("x-amz-date", "20130524T000000Z")
            .header("authorization", "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;x-amz-content-sha256;x-amz-date,Signature=0000")
            .body(())
            .unwrap();
        let (parts, _) = req.into_parts();
        assert_eq!(authenticate(&parts, &[], &[key()], now()).unwrap_err().code, ErrorCode::SignatureDoesNotMatch);
    }

    #[test]
    fn chunk_signatures() {
        // "Signature Calculations for the Authorization Header: Transferring Payload in
        // Multiple Chunks" example.
        let skey = signing_key("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "20130524", "us-east-1", "s3");
        let mut s = ChunkSigner {
            key: skey,
            amz_date: "20130524T000000Z".into(),
            scope: "20130524/us-east-1/s3/aws4_request".into(),
            prev: "4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9".into(),
        };
        let chunk1 = vec![b'a'; 65536];
        s.verify_chunk(&Sha256::digest(&chunk1), "ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648").unwrap();
        let chunk2 = vec![b'a'; 1024];
        s.verify_chunk(&Sha256::digest(&chunk2), "0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497").unwrap();
        s.verify_chunk(&Sha256::digest(b""), "b6c6ea8a5354eaf15b3cb7646744f4275b71ea724fed81ceb9323e279d449df9").unwrap();
    }
}
