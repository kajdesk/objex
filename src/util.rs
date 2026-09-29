use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, percent_decode_str, utf8_percent_encode};

/// Characters AWS leaves unescaped: A-Z a-z 0-9 - _ . ~
const AWS_STRICT: &AsciiSet = &NON_ALPHANUMERIC.remove(b'-').remove(b'_').remove(b'.').remove(b'~');
/// Same, but '/' is also kept (used for object key paths).
const AWS_PATH: &AsciiSet = &AWS_STRICT.remove(b'/');

/// URI-encode per the SigV4 rules (everything but unreserved characters).
pub fn uri_encode(s: &str) -> String {
    utf8_percent_encode(s, AWS_STRICT).to_string()
}

/// URI-encode, keeping '/' unescaped.
pub fn uri_encode_path(s: &str) -> String {
    utf8_percent_encode(s, AWS_PATH).to_string()
}

pub fn uri_decode(s: &str) -> Option<String> {
    percent_decode_str(s).decode_utf8().ok().map(|c| c.into_owned())
}

/// Random lowercase hex string of `bytes * 2` characters.
pub fn random_hex(bytes: usize) -> String {
    let mut buf = vec![0u8; bytes];
    rand::fill(&mut buf[..]);
    hex::encode(buf)
}

/// Random string from `alphabet`.
pub fn random_string(len: usize, alphabet: &[u8]) -> String {
    let mut buf = vec![0u8; len];
    rand::fill(&mut buf[..]);
    buf.iter().map(|b| alphabet[*b as usize % alphabet.len()] as char).collect()
}

/// ISO 8601 timestamp as used in S3 XML bodies, e.g. 2009-10-12T17:50:30.000Z
pub fn iso8601(t: &DateTime<Utc>) -> String {
    t.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

/// RFC 7231 HTTP date, e.g. Wed, 21 Oct 2015 07:28:00 GMT
pub fn http_date(t: &DateTime<Utc>) -> String {
    t.format("%a, %d %b %Y %H:%M:%S GMT").to_string()
}

pub fn parse_http_date(s: &str) -> Option<DateTime<Utc>> {
    let s = s.trim();
    if let Ok(t) = DateTime::parse_from_rfc2822(s) {
        return Some(t.with_timezone(&Utc));
    }
    for fmt in ["%a, %d %b %Y %H:%M:%S GMT", "%A, %d-%b-%y %H:%M:%S GMT", "%a %b %e %H:%M:%S %Y"] {
        if let Ok(t) = NaiveDateTime::parse_from_str(s, fmt) {
            return Some(Utc.from_utc_datetime(&t));
        }
    }
    DateTime::parse_from_rfc3339(s).ok().map(|t| t.with_timezone(&Utc))
}

/// SigV4 basic format: 20130524T000000Z
pub fn parse_amz_date(s: &str) -> Option<DateTime<Utc>> {
    NaiveDateTime::parse_from_str(s, "%Y%m%dT%H%M%SZ").ok().map(|t| Utc.from_utc_datetime(&t))
}

/// Escape text for inclusion in XML.
pub fn xml_escape(s: &str) -> std::borrow::Cow<'_, str> {
    if !s.contains(['&', '<', '>', '"', '\'']) && !s.chars().any(|c| (c as u32) < 0x20 && c != '\t' && c != '\n') {
        return s.into();
    }
    let mut out = String::with_capacity(s.len() + 16);
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            c if (c as u32) < 0x20 && c != '\t' && c != '\n' => out.push_str(&format!("&#x{:X};", c as u32)),
            c => out.push(c),
        }
    }
    out.into()
}

/// Validate an S3 bucket name (the modern, strict DNS-compatible rules).
pub fn valid_bucket_name(name: &str) -> bool {
    let b = name.as_bytes();
    if b.len() < 3 || b.len() > 63 {
        return false;
    }
    let ok_char = |c: u8| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-' || c == b'.';
    if !b.iter().all(|c| ok_char(*c)) {
        return false;
    }
    let alnum = |c: u8| c.is_ascii_lowercase() || c.is_ascii_digit();
    if !alnum(b[0]) || !alnum(b[b.len() - 1]) {
        return false;
    }
    if name.contains("..") || name.contains(".-") || name.contains("-.") {
        return false;
    }
    // must not look like an IPv4 address
    name.parse::<std::net::Ipv4Addr>().is_err()
}

/// Constant-time byte comparison.
pub fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_names() {
        assert!(valid_bucket_name("my-bucket.data"));
        assert!(!valid_bucket_name("My-Bucket"));
        assert!(!valid_bucket_name("ab"));
        assert!(!valid_bucket_name("192.168.1.1"));
        assert!(!valid_bucket_name("-abc"));
        assert!(!valid_bucket_name("a..b"));
    }

    #[test]
    fn encoding() {
        assert_eq!(uri_encode("a b/c~"), "a%20b%2Fc~");
        assert_eq!(uri_encode_path("a b/c+"), "a%20b/c%2B");
        assert_eq!(uri_decode("a%20b%2Fc").unwrap(), "a b/c");
    }

    #[test]
    fn dates() {
        let t = parse_http_date("Wed, 21 Oct 2015 07:28:00 GMT").unwrap();
        assert_eq!(http_date(&t), "Wed, 21 Oct 2015 07:28:00 GMT");
        assert_eq!(iso8601(&t), "2015-10-21T07:28:00.000Z");
        assert!(parse_amz_date("20130524T000000Z").is_some());
    }
}
