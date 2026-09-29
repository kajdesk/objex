//! Bucket CORS: OPTIONS preflight and headers on actual requests.

use http::request::Parts;
use http::{HeaderMap, StatusCode};

use super::{AppState, Resp, empty, set};
use crate::error::{ErrorCode, S3Error, S3Result};
use crate::storage::{BucketInfo, CorsRule};

/// Match with at most one '*' wildcard, as S3 allows in origins and headers.
fn wildcard(pattern: &str, value: &str, case_insensitive: bool) -> bool {
    let (p, v) = if case_insensitive { (pattern.to_ascii_lowercase(), value.to_ascii_lowercase()) } else { (pattern.to_string(), value.to_string()) };
    match p.split_once('*') {
        None => p == v,
        Some((pre, suf)) => v.len() >= pre.len() + suf.len() && v.starts_with(pre) && v.ends_with(suf),
    }
}

fn find_rule<'a>(rules: &'a [CorsRule], origin: &str, method: &str, headers: &[String]) -> Option<&'a CorsRule> {
    rules.iter().find(|r| {
        r.allowed_origins.iter().any(|o| wildcard(o, origin, false))
            && r.allowed_methods.iter().any(|m| m == method)
            && headers.iter().all(|h| r.allowed_headers.iter().any(|a| wildcard(a, h, true)))
    })
}

fn allow_origin(rule: &CorsRule, origin: &str) -> String {
    if rule.allowed_origins.iter().any(|o| o == "*") { "*".into() } else { origin.to_string() }
}

/// Answer an OPTIONS preflight request. No authentication is involved.
pub async fn preflight(state: &AppState, parts: &Parts, bucket: &str) -> S3Result<Resp> {
    let h = |n: &str| parts.headers.get(n).and_then(|v| v.to_str().ok());
    let (Some(origin), Some(method)) = (h("origin"), h("access-control-request-method")) else {
        return Err(S3Error::msg(ErrorCode::InvalidRequest, "Insufficient information. Origin request header needed."));
    };
    let forbidden = || S3Error::msg(ErrorCode::CORSResponse, "This CORS request is not allowed. This is usually because the evalution of Origin, request method / Access-Control-Request-Method or Access-Control-Request-Headers are not whitelisted by the resource's CORS spec.");
    if bucket.is_empty() {
        return Err(forbidden());
    }
    let info = state.store.get_bucket(bucket).await?;
    let requested: Vec<String> = h("access-control-request-headers")
        .map(|v| v.split(',').map(|s| s.trim().to_ascii_lowercase()).filter(|s| !s.is_empty()).collect())
        .unwrap_or_default();
    let rules = info.cors.as_deref().unwrap_or(&[]);
    let rule = find_rule(rules, origin, method, &requested).ok_or_else(forbidden)?;

    let mut r = empty(StatusCode::OK);
    let out = r.headers_mut();
    let allowed = allow_origin(rule, origin);
    if allowed != "*" {
        set(out, "access-control-allow-credentials", "true");
    }
    set(out, "access-control-allow-origin", allowed);
    set(out, "access-control-allow-methods", rule.allowed_methods.join(", "));
    if !requested.is_empty() {
        set(out, "access-control-allow-headers", requested.join(", "));
    }
    if !rule.expose_headers.is_empty() {
        set(out, "access-control-expose-headers", rule.expose_headers.join(", "));
    }
    if let Some(age) = rule.max_age_seconds {
        set(out, "access-control-max-age", age.to_string());
    }
    set(out, "vary", "Origin, Access-Control-Request-Headers, Access-Control-Request-Method");
    Ok(r)
}

/// Add CORS headers to a normal response when a bucket rule matches the origin.
pub fn apply(info: &BucketInfo, origin: &str, method: &str, out: &mut HeaderMap) {
    let Some(rules) = info.cors.as_deref() else { return };
    let Some(rule) = find_rule(rules, origin, method, &[]) else { return };
    let allowed = allow_origin(rule, origin);
    if allowed != "*" {
        set(out, "access-control-allow-credentials", "true");
    }
    set(out, "access-control-allow-origin", allowed);
    set(out, "access-control-allow-methods", rule.allowed_methods.join(", "));
    if !rule.expose_headers.is_empty() {
        set(out, "access-control-expose-headers", rule.expose_headers.join(", "));
    }
    if let Some(age) = rule.max_age_seconds {
        set(out, "access-control-max-age", age.to_string());
    }
    set(out, "vary", "Origin, Access-Control-Request-Headers, Access-Control-Request-Method");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn patterns() {
        assert!(wildcard("*", "https://a.com", false));
        assert!(wildcard("https://*.example.com", "https://app.example.com", false));
        assert!(!wildcard("https://*.example.com", "https://example.org", false));
        assert!(wildcard("X-Amz-*", "x-amz-date", true));
        let rule = CorsRule { allowed_origins: vec!["https://a.com".into()], allowed_methods: vec!["GET".into()], allowed_headers: vec!["*".into()], ..Default::default() };
        assert!(find_rule(std::slice::from_ref(&rule), "https://a.com", "GET", &["x-foo".into()]).is_some());
        assert!(find_rule(std::slice::from_ref(&rule), "https://a.com", "PUT", &[]).is_none());
    }
}
