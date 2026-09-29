//! S3 XML: a small writer for responses and serde models for request bodies.

use serde::Deserialize;

use crate::checksum::{Checksum, ChecksumAlgo};
use crate::error::{ErrorCode, S3Error, S3Result};
use crate::storage::CorsRule;
use crate::util::xml_escape;

pub const S3_NS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";

/// Builds an XML document element by element.
pub struct XmlWriter {
    out: String,
    stack: Vec<&'static str>,
}

impl XmlWriter {
    /// Start a document whose root element carries the S3 namespace.
    pub fn new(root: &'static str) -> Self {
        let mut w = XmlWriter { out: String::with_capacity(512), stack: Vec::new() };
        w.out.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        w.out.push_str(&format!("<{root} xmlns=\"{S3_NS}\">"));
        w.stack.push(root);
        w
    }

    /// Start a document whose root element has no namespace.
    pub fn bare(root: &'static str) -> Self {
        let mut w = XmlWriter { out: String::with_capacity(256), stack: Vec::new() };
        w.out.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        w.out.push_str(&format!("<{root}>"));
        w.stack.push(root);
        w
    }

    pub fn open(&mut self, name: &'static str) -> &mut Self {
        self.out.push('<');
        self.out.push_str(name);
        self.out.push('>');
        self.stack.push(name);
        self
    }

    pub fn close(&mut self) -> &mut Self {
        let name = self.stack.pop().expect("unbalanced close");
        self.out.push_str("</");
        self.out.push_str(name);
        self.out.push('>');
        self
    }

    pub fn elem(&mut self, name: &str, text: impl AsRef<str>) -> &mut Self {
        self.out.push('<');
        self.out.push_str(name);
        self.out.push('>');
        self.out.push_str(&xml_escape(text.as_ref()));
        self.out.push_str("</");
        self.out.push_str(name);
        self.out.push('>');
        self
    }

    pub fn opt(&mut self, name: &str, text: Option<impl AsRef<str>>) -> &mut Self {
        if let Some(t) = text {
            self.elem(name, t);
        }
        self
    }

    /// Insert pre-built markup.
    pub fn raw(&mut self, markup: &str) -> &mut Self {
        self.out.push_str(markup);
        self
    }

    pub fn checksum(&mut self, c: Option<&Checksum>) -> &mut Self {
        if let Some(c) = c {
            self.elem(c.algo.xml_tag(), &c.value);
        }
        self
    }

    pub fn finish(mut self) -> String {
        while !self.stack.is_empty() {
            self.close();
        }
        self.out
    }
}

pub fn parse<'a, T: Deserialize<'a>>(body: &'a [u8]) -> S3Result<T> {
    let s = std::str::from_utf8(body).map_err(|_| ErrorCode::MalformedXML)?;
    quick_xml::de::from_str(s).map_err(|e| S3Error::msg(ErrorCode::MalformedXML, format!("{} ({e})", ErrorCode::MalformedXML.default_message())))
}

#[derive(Debug, Deserialize)]
pub struct CompleteMultipartUpload {
    #[serde(rename = "Part", default)]
    pub parts: Vec<CompletedPart>,
}

#[derive(Debug, Deserialize)]
pub struct CompletedPart {
    #[serde(rename = "PartNumber")]
    pub number: u32,
    #[serde(rename = "ETag")]
    pub etag: String,
    #[serde(rename = "ChecksumCRC32")]
    pub crc32: Option<String>,
    #[serde(rename = "ChecksumCRC32C")]
    pub crc32c: Option<String>,
    #[serde(rename = "ChecksumCRC64NVME")]
    pub crc64nvme: Option<String>,
    #[serde(rename = "ChecksumSHA1")]
    pub sha1: Option<String>,
    #[serde(rename = "ChecksumSHA256")]
    pub sha256: Option<String>,
}

impl CompletedPart {
    pub fn checksum(&self) -> Option<Checksum> {
        [
            (ChecksumAlgo::Crc32, &self.crc32),
            (ChecksumAlgo::Crc32c, &self.crc32c),
            (ChecksumAlgo::Crc64nvme, &self.crc64nvme),
            (ChecksumAlgo::Sha1, &self.sha1),
            (ChecksumAlgo::Sha256, &self.sha256),
        ]
        .into_iter()
        .find_map(|(algo, v)| v.as_ref().map(|v| Checksum { algo, value: v.trim().to_string() }))
    }
}

#[derive(Debug, Deserialize)]
pub struct Delete {
    #[serde(rename = "Quiet", default)]
    pub quiet: bool,
    #[serde(rename = "Object", default)]
    pub objects: Vec<DeleteObject>,
}

#[derive(Debug, Deserialize)]
pub struct DeleteObject {
    #[serde(rename = "Key")]
    pub key: String,
}

#[derive(Debug, Deserialize)]
pub struct CorsConfiguration {
    #[serde(rename = "CORSRule", default)]
    pub rules: Vec<XCorsRule>,
}

#[derive(Debug, Deserialize)]
pub struct XCorsRule {
    #[serde(rename = "ID")]
    pub id: Option<String>,
    #[serde(rename = "AllowedOrigin", default)]
    pub allowed_origins: Vec<String>,
    #[serde(rename = "AllowedMethod", default)]
    pub allowed_methods: Vec<String>,
    #[serde(rename = "AllowedHeader", default)]
    pub allowed_headers: Vec<String>,
    #[serde(rename = "ExposeHeader", default)]
    pub expose_headers: Vec<String>,
    #[serde(rename = "MaxAgeSeconds")]
    pub max_age_seconds: Option<u32>,
}

impl From<XCorsRule> for CorsRule {
    fn from(r: XCorsRule) -> Self {
        CorsRule {
            id: r.id,
            allowed_origins: r.allowed_origins,
            allowed_methods: r.allowed_methods,
            allowed_headers: r.allowed_headers,
            expose_headers: r.expose_headers,
            max_age_seconds: r.max_age_seconds,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn complete_body() {
        let body = br#"<CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Part><ETag>"abc"</ETag><PartNumber>1</PartNumber><ChecksumCRC32>AAAAAA==</ChecksumCRC32></Part>
            <Part><PartNumber>2</PartNumber><ETag>def</ETag></Part>
        </CompleteMultipartUpload>"#;
        let c: CompleteMultipartUpload = parse(body).unwrap();
        assert_eq!(c.parts.len(), 2);
        assert_eq!(c.parts[0].etag, "\"abc\"");
        assert_eq!(c.parts[0].checksum().unwrap().algo, ChecksumAlgo::Crc32);
        assert_eq!(c.parts[1].number, 2);
    }

    #[test]
    fn delete_body() {
        let d: Delete = parse(b"<Delete><Quiet>true</Quiet><Object><Key>a &amp; b</Key></Object><Object><Key>c</Key><VersionId>x</VersionId></Object></Delete>").unwrap();
        assert!(d.quiet);
        assert_eq!(d.objects[0].key, "a & b");
        assert_eq!(d.objects.len(), 2);
    }

    #[test]
    fn cors_body() {
        let c: CorsConfiguration = parse(b"<CORSConfiguration><CORSRule><AllowedOrigin>*</AllowedOrigin><AllowedMethod>GET</AllowedMethod><AllowedMethod>PUT</AllowedMethod><MaxAgeSeconds>300</MaxAgeSeconds></CORSRule></CORSConfiguration>").unwrap();
        assert_eq!(c.rules[0].allowed_methods, ["GET", "PUT"]);
        assert_eq!(c.rules[0].max_age_seconds, Some(300));
    }

    #[test]
    fn writer() {
        let mut w = XmlWriter::new("Root");
        w.open("A").elem("B", "x<y").close();
        assert!(w.finish().ends_with("<Root xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><A><B>x&lt;y</B></A></Root>"));
    }
}
