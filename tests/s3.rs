//! End-to-end tests: a real server driven by the official AWS SDK.

use std::sync::Arc;
use std::time::Duration;

use aws_credential_types::Credentials;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{BehaviorVersion, Region};
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    ChecksumAlgorithm, ChecksumMode, CompletedMultipartUpload, CompletedPart, CorsConfiguration, CorsRule, Delete, ObjectIdentifier,
};
use objex::config::KeyConfig;
use objex::s3::AppState;
use objex::storage::local::LocalEngine;

const AK: &str = "OBXTESTACCESSKEY0001";
const SK: &str = "testsecretkey0000000000000000000000000000";
const RO_AK: &str = "OBXTESTREADONLY00001";

struct Server {
    endpoint: String,
    client: Client,
    _dir: TempDir,
}

struct TempDir(std::path::PathBuf);

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn client_for(endpoint: &str, ak: &str, sk: &str) -> Client {
    let conf = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .endpoint_url(endpoint)
        .region(Region::new("auto"))
        .credentials_provider(Credentials::new(ak, sk, None, None, "test"))
        .force_path_style(true)
        .build();
    Client::from_conf(conf)
}

async fn start() -> Server {
    let dir = std::env::temp_dir().join(format!("objex-it-{}", objex::util::random_hex(8)));
    let engine = Arc::new(LocalEngine::open(&dir, false).unwrap());
    let keys = vec![
        KeyConfig { name: "admin".into(), access_key: AK.into(), secret_key: SK.into(), buckets: vec![], read_only: false },
        KeyConfig { name: "ro".into(), access_key: RO_AK.into(), secret_key: SK.into(), buckets: vec![], read_only: true },
    ];
    let state = Arc::new(AppState::new(engine, keys, "us-east-1".into(), String::new()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(objex::server::serve(listener, state, std::future::pending()));
    Server { client: client_for(&endpoint, AK, SK), endpoint, _dir: TempDir(dir) }
}

fn code<E: ProvideErrorMetadata>(e: &E) -> String {
    e.code().unwrap_or("").to_string()
}

async fn body(out: aws_sdk_s3::operation::get_object::GetObjectOutput) -> Vec<u8> {
    out.body.collect().await.unwrap().into_bytes().to_vec()
}

fn data(n: usize, seed: u8) -> Vec<u8> {
    (0..n).map(|i| ((i * 31) as u8).wrapping_add(seed)).collect()
}

#[tokio::test]
async fn buckets() {
    let s = start().await;
    let c = &s.client;
    c.create_bucket().bucket("alpha").send().await.unwrap();
    c.create_bucket().bucket("beta").send().await.unwrap();
    let err = c.create_bucket().bucket("alpha").send().await.unwrap_err();
    assert_eq!(code(err.as_service_error().unwrap()), "BucketAlreadyOwnedByYou");
    let err = c.create_bucket().bucket("Bad_Name").send().await.unwrap_err();
    assert_eq!(code(err.as_service_error().unwrap()), "InvalidBucketName");

    let list = c.list_buckets().send().await.unwrap();
    let names: Vec<_> = list.buckets().iter().filter_map(|b| b.name()).collect();
    assert_eq!(names, ["alpha", "beta"]);

    c.head_bucket().bucket("alpha").send().await.unwrap();
    assert!(c.head_bucket().bucket("nope").send().await.is_err());
    c.get_bucket_location().bucket("alpha").send().await.unwrap();
    c.get_bucket_versioning().bucket("alpha").send().await.unwrap();

    c.put_object().bucket("beta").key("x").body(ByteStream::from_static(b"x")).send().await.unwrap();
    let err = c.delete_bucket().bucket("beta").send().await.unwrap_err();
    assert_eq!(code(err.as_service_error().unwrap()), "BucketNotEmpty");
    c.delete_object().bucket("beta").key("x").send().await.unwrap();
    c.delete_bucket().bucket("beta").send().await.unwrap();
    let err = c.list_objects_v2().bucket("beta").send().await.unwrap_err();
    assert_eq!(code(err.as_service_error().unwrap()), "NoSuchBucket");
}

#[tokio::test]
async fn objects() {
    let s = start().await;
    let c = &s.client;
    c.create_bucket().bucket("objs").send().await.unwrap();

    let put = c
        .put_object()
        .bucket("objs")
        .key("dir/hello world+ü.txt")
        .content_type("text/plain")
        .cache_control("max-age=60")
        .metadata("color", "blue")
        .body(ByteStream::from_static(b"hello, world"))
        .send()
        .await
        .unwrap();
    let etag = put.e_tag().unwrap().to_string();
    assert_eq!(etag, "\"e4d7f1b4ed2e42d15898f4b27b019da4\"");

    let got = c.get_object().bucket("objs").key("dir/hello world+ü.txt").send().await.unwrap();
    assert_eq!(got.content_type(), Some("text/plain"));
    assert_eq!(got.cache_control(), Some("max-age=60"));
    assert_eq!(got.metadata().unwrap().get("color").map(String::as_str), Some("blue"));
    assert_eq!(got.content_length(), Some(12));
    assert_eq!(body(got).await, b"hello, world");

    let head = c.head_object().bucket("objs").key("dir/hello world+ü.txt").send().await.unwrap();
    assert_eq!(head.e_tag(), Some(etag.as_str()));
    assert_eq!(head.content_length(), Some(12));

    // Range reads
    let r = c.get_object().bucket("objs").key("dir/hello world+ü.txt").range("bytes=7-").send().await.unwrap();
    assert_eq!(r.content_range(), Some("bytes 7-11/12"));
    assert_eq!(body(r).await, b"world");
    let r = c.get_object().bucket("objs").key("dir/hello world+ü.txt").range("bytes=-5").send().await.unwrap();
    assert_eq!(body(r).await, b"world");
    let err = c.get_object().bucket("objs").key("dir/hello world+ü.txt").range("bytes=100-").send().await.unwrap_err();
    assert_eq!(code(err.as_service_error().unwrap()), "InvalidRange");

    // Response overrides
    let r = c.get_object().bucket("objs").key("dir/hello world+ü.txt").response_content_type("application/json").send().await.unwrap();
    assert_eq!(r.content_type(), Some("application/json"));

    // Conditional reads
    let err = c.get_object().bucket("objs").key("dir/hello world+ü.txt").if_match("\"nope\"").send().await.unwrap_err();
    assert_eq!(err.raw_response().unwrap().status().as_u16(), 412);
    let err = c.get_object().bucket("objs").key("dir/hello world+ü.txt").if_none_match(&etag).send().await.unwrap_err();
    assert_eq!(err.raw_response().unwrap().status().as_u16(), 304);
    c.get_object().bucket("objs").key("dir/hello world+ü.txt").if_match(&etag).send().await.unwrap();

    // Conditional write
    let err = c.put_object().bucket("objs").key("dir/hello world+ü.txt").if_none_match("*").body(ByteStream::from_static(b"no")).send().await.unwrap_err();
    assert_eq!(code(err.as_service_error().unwrap()), "PreconditionFailed");

    // Checksums: explicit SHA256, then read back with checksum mode
    let put = c
        .put_object()
        .bucket("objs")
        .key("sum")
        .checksum_algorithm(ChecksumAlgorithm::Sha256)
        .body(ByteStream::from_static(b"checksummed"))
        .send()
        .await
        .unwrap();
    assert!(put.checksum_sha256().is_some());
    let got = c.get_object().bucket("objs").key("sum").checksum_mode(ChecksumMode::Enabled).send().await.unwrap();
    assert_eq!(got.checksum_sha256(), put.checksum_sha256());
    assert_eq!(body(got).await, b"checksummed");
    for algo in [ChecksumAlgorithm::Crc32, ChecksumAlgorithm::Crc32C, ChecksumAlgorithm::Crc64Nvme, ChecksumAlgorithm::Sha1] {
        let put = c.put_object().bucket("objs").key("sum2").checksum_algorithm(algo.clone()).body(ByteStream::from(data(100_000, 3))).send().await.unwrap();
        let got = c.get_object().bucket("objs").key("sum2").checksum_mode(ChecksumMode::Enabled).send().await.unwrap();
        assert_eq!(body(got).await, data(100_000, 3), "{algo:?}");
        assert!(put.checksum_crc32().is_some() || put.checksum_crc32_c().is_some() || put.checksum_crc64_nvme().is_some() || put.checksum_sha1().is_some());
    }

    // A larger object, streamed
    let big = data(3 * 1024 * 1024 + 17, 9);
    c.put_object().bucket("objs").key("big").body(ByteStream::from(big.clone())).send().await.unwrap();
    let got = c.get_object().bucket("objs").key("big").send().await.unwrap();
    assert_eq!(body(got).await, big);

    // Empty object
    c.put_object().bucket("objs").key("empty").body(ByteStream::from_static(b"")).send().await.unwrap();
    let got = c.get_object().bucket("objs").key("empty").send().await.unwrap();
    assert_eq!(body(got).await, b"");

    c.delete_object().bucket("objs").key("dir/hello world+ü.txt").send().await.unwrap();
    let err = c.get_object().bucket("objs").key("dir/hello world+ü.txt").send().await.unwrap_err();
    assert_eq!(code(err.as_service_error().unwrap()), "NoSuchKey");
    c.delete_object().bucket("objs").key("never-existed").send().await.unwrap();
}

#[tokio::test]
async fn listing() {
    let s = start().await;
    let c = &s.client;
    c.create_bucket().bucket("lists").send().await.unwrap();
    let keys = ["a.txt", "photos/2024/1.jpg", "photos/2024/2.jpg", "photos/2025/1.jpg", "photos/cover.jpg", "z z", "ü"];
    for k in keys {
        c.put_object().bucket("lists").key(k).body(ByteStream::from_static(b"x")).send().await.unwrap();
    }

    let all: Vec<String> = c
        .list_objects_v2()
        .bucket("lists")
        .max_keys(2)
        .into_paginator()
        .send()
        .collect::<Result<Vec<_>, _>>()
        .await
        .unwrap()
        .iter()
        .flat_map(|p| p.contents().iter().map(|o| o.key().unwrap().to_string()))
        .collect();
    assert_eq!(all, keys);

    let r = c.list_objects_v2().bucket("lists").delimiter("/").send().await.unwrap();
    let files: Vec<_> = r.contents().iter().map(|o| o.key().unwrap()).collect();
    let dirs: Vec<_> = r.common_prefixes().iter().map(|p| p.prefix().unwrap()).collect();
    assert_eq!(files, ["a.txt", "z z", "ü"]);
    assert_eq!(dirs, ["photos/"]);
    assert_eq!(r.key_count(), Some(4));

    let r = c.list_objects_v2().bucket("lists").prefix("photos/").delimiter("/").send().await.unwrap();
    let dirs: Vec<_> = r.common_prefixes().iter().map(|p| p.prefix().unwrap()).collect();
    assert_eq!(dirs, ["photos/2024/", "photos/2025/"]);
    assert_eq!(r.contents().len(), 1);

    let r = c.list_objects_v2().bucket("lists").start_after("photos/2025/1.jpg").send().await.unwrap();
    assert_eq!(r.contents().len(), 3);

    // V1 with markers
    let r = c.list_objects().bucket("lists").max_keys(3).send().await.unwrap();
    assert_eq!(r.is_truncated(), Some(true));
    let r = c.list_objects().bucket("lists").marker(r.next_marker().unwrap_or("photos/2024/2.jpg")).send().await.unwrap();
    assert_eq!(r.contents().len(), 4);
}

#[tokio::test]
async fn multipart() {
    let s = start().await;
    let c = &s.client;
    c.create_bucket().bucket("mpu").send().await.unwrap();

    for algo in [None, Some(ChecksumAlgorithm::Crc32), Some(ChecksumAlgorithm::Crc64Nvme), Some(ChecksumAlgorithm::Sha256)] {
        let key = format!("big-{algo:?}");
        let mut req = c.create_multipart_upload().bucket("mpu").key(&key).content_type("video/mp4");
        if let Some(a) = &algo {
            req = req.checksum_algorithm(a.clone());
        }
        let up = req.send().await.unwrap();
        let id = up.upload_id().unwrap();
        let parts = [data(5 * 1024 * 1024, 1), data(5 * 1024 * 1024, 2), data(1234, 3)];
        let mut done = Vec::new();
        for (i, p) in parts.iter().enumerate() {
            let mut req = c.upload_part().bucket("mpu").key(&key).upload_id(id).part_number(i as i32 + 1).body(ByteStream::from(p.clone()));
            if let Some(a) = &algo {
                req = req.checksum_algorithm(a.clone());
            }
            let r = req.send().await.unwrap();
            done.push(
                CompletedPart::builder()
                    .part_number(i as i32 + 1)
                    .e_tag(r.e_tag().unwrap())
                    .set_checksum_crc32(r.checksum_crc32().map(Into::into))
                    .set_checksum_crc64_nvme(r.checksum_crc64_nvme().map(Into::into))
                    .set_checksum_sha256(r.checksum_sha256().map(Into::into))
                    .build(),
            );
        }
        let listed = c.list_parts().bucket("mpu").key(&key).upload_id(id).send().await.unwrap();
        assert_eq!(listed.parts().len(), 3);
        let uploads = c.list_multipart_uploads().bucket("mpu").send().await.unwrap();
        assert_eq!(uploads.uploads().len(), 1);

        let out = c
            .complete_multipart_upload()
            .bucket("mpu")
            .key(&key)
            .upload_id(id)
            .multipart_upload(CompletedMultipartUpload::builder().set_parts(Some(done)).build())
            .send()
            .await
            .unwrap();
        assert!(out.e_tag().unwrap().ends_with("-3\""));

        let got = c.get_object().bucket("mpu").key(&key).checksum_mode(ChecksumMode::Enabled).send().await.unwrap();
        assert_eq!(got.content_type(), Some("video/mp4"));
        assert_eq!(got.content_length(), Some(10 * 1024 * 1024 + 1234));
        let all = body(got).await;
        assert_eq!(all, parts.concat());

        let p2 = c.get_object().bucket("mpu").key(&key).part_number(2).send().await.unwrap();
        assert_eq!(p2.parts_count(), Some(3));
        assert_eq!(body(p2).await, parts[1]);
    }

    // abort
    let up = c.create_multipart_upload().bucket("mpu").key("gone").send().await.unwrap();
    let id = up.upload_id().unwrap();
    c.upload_part().bucket("mpu").key("gone").upload_id(id).part_number(1).body(ByteStream::from_static(b"x")).send().await.unwrap();
    c.abort_multipart_upload().bucket("mpu").key("gone").upload_id(id).send().await.unwrap();
    let err = c.list_parts().bucket("mpu").key("gone").upload_id(id).send().await.unwrap_err();
    assert_eq!(code(err.as_service_error().unwrap()), "NoSuchUpload");

    // too-small part
    let up = c.create_multipart_upload().bucket("mpu").key("small").send().await.unwrap();
    let id = up.upload_id().unwrap();
    let mut parts = Vec::new();
    for n in 1..=2 {
        let r = c.upload_part().bucket("mpu").key("small").upload_id(id).part_number(n).body(ByteStream::from_static(b"tiny")).send().await.unwrap();
        parts.push(CompletedPart::builder().part_number(n).e_tag(r.e_tag().unwrap()).build());
    }
    let err = c
        .complete_multipart_upload()
        .bucket("mpu")
        .key("small")
        .upload_id(id)
        .multipart_upload(CompletedMultipartUpload::builder().set_parts(Some(parts)).build())
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(err.as_service_error().unwrap()), "EntityTooSmall");
}

#[tokio::test]
async fn copies_and_batch_delete() {
    let s = start().await;
    let c = &s.client;
    c.create_bucket().bucket("src-b").send().await.unwrap();
    c.create_bucket().bucket("dst-b").send().await.unwrap();
    c.put_object().bucket("src-b").key("orig file").metadata("k", "v").body(ByteStream::from(data(10_000, 5))).send().await.unwrap();

    c.copy_object().bucket("dst-b").key("copy").copy_source("src-b/orig%20file").send().await.unwrap();
    let got = c.get_object().bucket("dst-b").key("copy").send().await.unwrap();
    assert_eq!(got.metadata().unwrap().get("k").map(String::as_str), Some("v"));
    assert_eq!(body(got).await, data(10_000, 5));

    c.copy_object()
        .bucket("dst-b")
        .key("copy")
        .copy_source("dst-b/copy")
        .metadata_directive(aws_sdk_s3::types::MetadataDirective::Replace)
        .content_type("image/png")
        .send()
        .await
        .unwrap();
    let head = c.head_object().bucket("dst-b").key("copy").send().await.unwrap();
    assert_eq!(head.content_type(), Some("image/png"));
    assert!(head.metadata().is_none_or(|m| m.is_empty()));

    let err = c.copy_object().bucket("dst-b").key("x").copy_source("src-b/orig%20file").copy_source_if_match("\"no\"").send().await.unwrap_err();
    assert_eq!(code(err.as_service_error().unwrap()), "PreconditionFailed");

    // UploadPartCopy with ranges
    let up = c.create_multipart_upload().bucket("dst-b").key("assembled").send().await.unwrap();
    let id = up.upload_id().unwrap();
    let r = c.upload_part_copy().bucket("dst-b").key("assembled").upload_id(id).part_number(1).copy_source("src-b/orig%20file").copy_source_range("bytes=0-99").send().await.unwrap();
    let etag = r.copy_part_result().unwrap().e_tag().unwrap().to_string();
    c.complete_multipart_upload()
        .bucket("dst-b")
        .key("assembled")
        .upload_id(id)
        .multipart_upload(CompletedMultipartUpload::builder().parts(CompletedPart::builder().part_number(1).e_tag(etag).build()).build())
        .send()
        .await
        .unwrap();
    let got = c.get_object().bucket("dst-b").key("assembled").send().await.unwrap();
    assert_eq!(body(got).await, &data(10_000, 5)[..100]);

    // DeleteObjects
    for k in ["d1", "d2", "d3"] {
        c.put_object().bucket("dst-b").key(k).body(ByteStream::from_static(b"x")).send().await.unwrap();
    }
    let del = Delete::builder()
        .set_objects(Some(["d1", "d2", "missing"].iter().map(|k| ObjectIdentifier::builder().key(*k).build().unwrap()).collect()))
        .build()
        .unwrap();
    let out = c.delete_objects().bucket("dst-b").delete(del).send().await.unwrap();
    assert_eq!(out.deleted().len(), 3);
    let left: Vec<_> = c.list_objects_v2().bucket("dst-b").prefix("d").send().await.unwrap().contents().iter().map(|o| o.key().unwrap().to_string()).collect();
    assert_eq!(left, ["d3"]);
}

#[tokio::test]
async fn access_control() {
    let s = start().await;
    let c = &s.client;
    c.create_bucket().bucket("private-b").send().await.unwrap();
    c.put_object().bucket("private-b").key("secret.txt").body(ByteStream::from_static(b"s3cr3t")).send().await.unwrap();
    let http = reqwest::Client::new();

    // anonymous access is denied
    let r = http.get(format!("{}/private-b/secret.txt", s.endpoint)).send().await.unwrap();
    assert_eq!(r.status().as_u16(), 403);
    assert!(r.text().await.unwrap().contains("<Code>AccessDenied</Code>"));

    // wrong secret
    let bad = client_for(&s.endpoint, AK, "wrong-secret");
    let err = bad.list_buckets().send().await.unwrap_err();
    assert_eq!(code(err.as_service_error().unwrap()), "SignatureDoesNotMatch");
    let unknown = client_for(&s.endpoint, "OBXNOSUCHKEY", SK);
    let err = unknown.list_buckets().send().await.unwrap_err();
    assert_eq!(code(err.as_service_error().unwrap()), "InvalidAccessKeyId");

    // presigned GET and PUT
    let cfg = PresigningConfig::expires_in(Duration::from_secs(300)).unwrap();
    let url = c.get_object().bucket("private-b").key("secret.txt").presigned(cfg.clone()).await.unwrap();
    let r = http.get(url.uri()).send().await.unwrap();
    assert_eq!(r.status().as_u16(), 200);
    assert_eq!(r.text().await.unwrap(), "s3cr3t");
    let url = c.put_object().bucket("private-b").key("via-presign").presigned(cfg).await.unwrap();
    let r = http.put(url.uri()).body("uploaded").send().await.unwrap();
    assert_eq!(r.status().as_u16(), 200);
    let got = c.get_object().bucket("private-b").key("via-presign").send().await.unwrap();
    assert_eq!(body(got).await, b"uploaded");

    // read-only key
    let ro = client_for(&s.endpoint, RO_AK, SK);
    ro.get_object().bucket("private-b").key("secret.txt").send().await.unwrap();
    let err = ro.put_object().bucket("private-b").key("x").body(ByteStream::from_static(b"x")).send().await.unwrap_err();
    assert_eq!(code(err.as_service_error().unwrap()), "AccessDenied");

    // public-read bucket
    c.create_bucket().bucket("public-b").acl(aws_sdk_s3::types::BucketCannedAcl::PublicRead).send().await.unwrap();
    c.put_object().bucket("public-b").key("hi.txt").body(ByteStream::from_static(b"hi")).send().await.unwrap();
    let r = http.get(format!("{}/public-b/hi.txt", s.endpoint)).send().await.unwrap();
    assert_eq!(r.status().as_u16(), 200);
    assert_eq!(r.text().await.unwrap(), "hi");
    let r = http.put(format!("{}/public-b/hi.txt", s.endpoint)).body("nope").send().await.unwrap();
    assert_eq!(r.status().as_u16(), 403);
    let acl = c.get_bucket_acl().bucket("public-b").send().await.unwrap();
    assert_eq!(acl.grants().len(), 2);
    c.put_bucket_acl().bucket("public-b").acl(aws_sdk_s3::types::BucketCannedAcl::Private).send().await.unwrap();
    let r = http.get(format!("{}/public-b/hi.txt", s.endpoint)).send().await.unwrap();
    assert_eq!(r.status().as_u16(), 403);
}

#[tokio::test]
async fn cors() {
    let s = start().await;
    let c = &s.client;
    c.create_bucket().bucket("web").send().await.unwrap();
    let err = c.get_bucket_cors().bucket("web").send().await.unwrap_err();
    assert_eq!(code(err.as_service_error().unwrap()), "NoSuchCORSConfiguration");

    let rule = CorsRule::builder()
        .allowed_origins("https://app.example.com")
        .allowed_methods("GET")
        .allowed_methods("PUT")
        .allowed_headers("*")
        .expose_headers("ETag")
        .max_age_seconds(600)
        .build()
        .unwrap();
    c.put_bucket_cors().bucket("web").cors_configuration(CorsConfiguration::builder().cors_rules(rule).build().unwrap()).send().await.unwrap();
    let got = c.get_bucket_cors().bucket("web").send().await.unwrap();
    assert_eq!(got.cors_rules()[0].allowed_methods(), ["GET", "PUT"]);

    let http = reqwest::Client::new();
    let url = format!("{}/web/file.txt", s.endpoint);
    let r = http
        .request(reqwest::Method::OPTIONS, &url)
        .header("origin", "https://app.example.com")
        .header("access-control-request-method", "PUT")
        .header("access-control-request-headers", "content-type, x-amz-date")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 200);
    assert_eq!(r.headers()["access-control-allow-origin"], "https://app.example.com");
    assert_eq!(r.headers()["access-control-max-age"], "600");

    let r = http.request(reqwest::Method::OPTIONS, &url).header("origin", "https://evil.example").header("access-control-request-method", "GET").send().await.unwrap();
    assert_eq!(r.status().as_u16(), 403);

    c.put_object().bucket("web").key("file.txt").body(ByteStream::from_static(b"x")).send().await.unwrap();
    let presigned = c.get_object().bucket("web").key("file.txt").presigned(PresigningConfig::expires_in(Duration::from_secs(60)).unwrap()).await.unwrap();
    let r = http.get(presigned.uri()).header("origin", "https://app.example.com").send().await.unwrap();
    assert_eq!(r.headers()["access-control-allow-origin"], "https://app.example.com");
    assert_eq!(r.headers()["access-control-expose-headers"], "ETag");

    c.delete_bucket_cors().bucket("web").send().await.unwrap();
    assert!(c.get_bucket_cors().bucket("web").send().await.is_err());
}

// ---------------------------------------------------------------------------
// aws-chunked uploads, built by hand (the SDK only streams chunks over HTTPS)
// ---------------------------------------------------------------------------

mod chunked {
    use super::*;
    use aws_sigv4::http_request::{PercentEncodingMode, SignableBody, SignableRequest, SigningSettings, UriPathNormalizationMode, sign};
    use aws_sigv4::sign::v4;
    use base64::Engine as _;
    use hmac::{Hmac, KeyInit, Mac};
    use sha2::{Digest, Sha256};
    use std::time::SystemTime;

    fn hmac(key: &[u8], data: &str) -> Vec<u8> {
        let mut m = Hmac::<Sha256>::new_from_slice(key).unwrap();
        m.update(data.as_bytes());
        m.finalize().into_bytes().to_vec()
    }

    /// Sign the request headers; returns the headers to send and the seed signature.
    fn sign_headers(method: &str, url: &str, headers: &[(&str, String)], payload: &str, now: SystemTime) -> (Vec<(String, String)>, String) {
        let identity = Credentials::new(AK, SK, None, None, "test").into();
        let mut settings = SigningSettings::default();
        settings.percent_encoding_mode = PercentEncodingMode::Single;
        settings.uri_path_normalization_mode = UriPathNormalizationMode::Disabled;
        settings.payload_checksum_kind = aws_sigv4::http_request::PayloadChecksumKind::XAmzSha256;
        let params = v4::SigningParams::builder().identity(&identity).region("auto").name("s3").time(now).settings(settings).build().unwrap().into();
        let req = SignableRequest::new(method, url, headers.iter().map(|(k, v)| (*k, v.as_str())), SignableBody::Precomputed(payload.into())).unwrap();
        let (instructions, signature) = sign(req, &params).unwrap().into_parts();
        let (hdrs, _) = instructions.into_parts();
        let mut out: Vec<(String, String)> = headers.iter().map(|(k, v)| (k.to_string(), v.clone())).collect();
        out.extend(hdrs.into_iter().map(|h| (h.name().to_string(), h.value().to_string())));
        (out, signature)
    }

    struct ChunkSigner {
        key: Vec<u8>,
        amz_date: String,
        scope: String,
        prev: String,
    }

    impl ChunkSigner {
        fn new(now: SystemTime, seed: String) -> Self {
            let t: chrono::DateTime<chrono::Utc> = now.into();
            let date = t.format("%Y%m%d").to_string();
            let k = hmac(format!("AWS4{SK}").as_bytes(), &date);
            let k = hmac(&k, "auto");
            let k = hmac(&k, "s3");
            let key = hmac(&k, "aws4_request");
            ChunkSigner { key, amz_date: t.format("%Y%m%dT%H%M%SZ").to_string(), scope: format!("{date}/auto/s3/aws4_request"), prev: seed }
        }

        fn chunk(&mut self, data: &[u8]) -> String {
            let sts = format!(
                "AWS4-HMAC-SHA256-PAYLOAD\n{}\n{}\n{}\n{}\n{}",
                self.amz_date,
                self.scope,
                self.prev,
                hex::encode(Sha256::digest(b"")),
                hex::encode(Sha256::digest(data))
            );
            self.prev = hex::encode(hmac(&self.key, &sts));
            self.prev.clone()
        }

        fn trailer(&mut self, canonical: &str) -> String {
            let sts = format!("AWS4-HMAC-SHA256-TRAILER\n{}\n{}\n{}\n{}", self.amz_date, self.scope, self.prev, hex::encode(Sha256::digest(canonical.as_bytes())));
            self.prev = hex::encode(hmac(&self.key, &sts));
            self.prev.clone()
        }
    }

    fn crc32_b64(data: &[u8]) -> String {
        base64::engine::general_purpose::STANDARD.encode(crc32fast::hash(data).to_be_bytes())
    }

    /// Upload `data` in `chunk` sized pieces. `mode` is the x-amz-content-sha256 value.
    async fn upload(s: &Server, key: &str, data: &[u8], chunk: usize, mode: &str, tamper: bool) -> reqwest::Response {
        let url = format!("{}/chunky/{key}", s.endpoint);
        let signed = mode.starts_with("STREAMING-AWS4");
        let trailer = mode.ends_with("TRAILER");
        let now = SystemTime::now();
        let mut headers = vec![
            ("content-encoding", "aws-chunked".to_string()),
            ("x-amz-decoded-content-length", data.len().to_string()),
        ];
        if trailer {
            headers.push(("x-amz-trailer", "x-amz-checksum-crc32".to_string()));
        }
        let host = s.endpoint.trim_start_matches("http://").to_string();
        headers.push(("host", host));
        let (hdrs, seed) = sign_headers("PUT", &url, &headers, mode, now);
        let mut signer = ChunkSigner::new(now, seed);

        let mut body = Vec::new();
        for piece in data.chunks(chunk).chain(std::iter::once(&[][..])) {
            body.extend_from_slice(format!("{:x}", piece.len()).as_bytes());
            if signed {
                body.extend_from_slice(format!(";chunk-signature={}", signer.chunk(piece)).as_bytes());
            }
            body.extend_from_slice(b"\r\n");
            if !piece.is_empty() {
                let mut p = piece.to_vec();
                if tamper {
                    p[0] ^= 1;
                }
                body.extend_from_slice(&p);
                body.extend_from_slice(b"\r\n");
            }
        }
        if trailer {
            let t = format!("x-amz-checksum-crc32:{}", crc32_b64(data));
            body.extend_from_slice(format!("{t}\r\n").as_bytes());
            if signed {
                body.extend_from_slice(format!("x-amz-trailer-signature:{}\r\n", signer.trailer(&format!("{t}\n"))).as_bytes());
            }
        }
        body.extend_from_slice(b"\r\n");

        let mut req = reqwest::Client::new().put(&url).body(body);
        for (k, v) in hdrs {
            if k != "host" {
                req = req.header(k, v);
            }
        }
        req.send().await.unwrap()
    }

    #[tokio::test]
    async fn streaming_uploads() {
        let s = start().await;
        s.client.create_bucket().bucket("chunky").send().await.unwrap();
        let data = data(200_000, 7);
        for mode in ["STREAMING-UNSIGNED-PAYLOAD-TRAILER", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"] {
            let r = upload(&s, mode, &data, 65536, mode, false).await;
            let status = r.status().as_u16();
            assert_eq!(status, 200, "{mode}: {}", r.text().await.unwrap());
            let got = s.client.get_object().bucket("chunky").key(mode).checksum_mode(ChecksumMode::Enabled).send().await.unwrap();
            if mode.ends_with("TRAILER") {
                assert_eq!(got.checksum_crc32(), Some(crc32_b64(&data).as_str()), "{mode}");
            }
            assert_eq!(got.content_encoding(), None);
            assert_eq!(body(got).await, data, "{mode}");
        }
        // A modified chunk fails its signature and nothing is stored.
        let r = upload(&s, "tampered", &data, 65536, "STREAMING-AWS4-HMAC-SHA256-PAYLOAD", true).await;
        assert_eq!(r.status().as_u16(), 403);
        let err = s.client.head_object().bucket("chunky").key("tampered").send().await.unwrap_err();
        assert_eq!(err.raw_response().unwrap().status().as_u16(), 404);
        // With an unsigned trailer a modified chunk fails the checksum.
        let r = upload(&s, "tampered", &data, 65536, "STREAMING-UNSIGNED-PAYLOAD-TRAILER", true).await;
        assert_eq!(r.status().as_u16(), 400);
    }
}
