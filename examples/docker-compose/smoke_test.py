"""End-to-end check of objex from another container, the way an app would use it."""

import gzip
import hashlib
import io
import json
import os
import urllib.request

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config

endpoint = os.environ["S3_ENDPOINT"]
creds = dict(aws_access_key_id=os.environ["S3_ACCESS_KEY"], aws_secret_access_key=os.environ["S3_SECRET_KEY"])
# Path-style addressing: "bucket.objex" does not resolve inside a compose network.
cfg = Config(s3={"addressing_style": "path"}, signature_version="s3v4")
s3 = boto3.client("s3", endpoint_url=endpoint, region_name="auto", config=cfg, **creds)


def check(name, cond):
    print(("ok   " if cond else "FAIL ") + name)
    if not cond:
        raise SystemExit(1)


# An image
png = b"\x89PNG\r\n\x1a\n" + os.urandom(200_000)
s3.put_object(Bucket="uploads", Key="images/cat.png", Body=png, ContentType="image/png", CacheControl="max-age=86400")
obj = s3.get_object(Bucket="uploads", Key="images/cat.png")
check("image round trip", obj["Body"].read() == png and obj["ContentType"] == "image/png")

# An rrweb recording chunk, stored gzipped
events = [{"type": 3, "timestamp": 1700000000000 + i, "data": {"source": 1, "x": i, "y": i}} for i in range(5000)]
blob = gzip.compress(json.dumps(events).encode())
s3.put_object(Bucket="uploads", Key="rrweb/session-1/000001.json.gz", Body=blob, ContentType="application/json", ContentEncoding="gzip")
got = s3.get_object(Bucket="uploads", Key="rrweb/session-1/000001.json.gz")
check("rrweb chunk round trip", json.loads(gzip.decompress(got["Body"].read())) == events)

# A video, uploaded in parallel 8 MiB parts
video = os.urandom(40 * 1024 * 1024)
tc = TransferConfig(multipart_threshold=8 * 1024 * 1024, multipart_chunksize=8 * 1024 * 1024, max_concurrency=4)
s3.upload_fileobj(io.BytesIO(video), "uploads", "videos/clip.mp4", ExtraArgs={"ContentType": "video/mp4"}, Config=tc)
head = s3.head_object(Bucket="uploads", Key="videos/clip.mp4")
check("multipart upload", head["ContentLength"] == len(video) and head["ETag"].endswith('-5"'))
out = io.BytesIO()
s3.download_fileobj("uploads", "videos/clip.mp4", out, Config=tc)
check("parallel ranged download", hashlib.sha256(out.getvalue()).digest() == hashlib.sha256(video).digest())
part = s3.get_object(Bucket="uploads", Key="videos/clip.mp4", Range="bytes=1000-1999")["Body"].read()
check("video seek (Range)", part == video[1000:2000])

# Listing like a file browser
page = s3.list_objects_v2(Bucket="uploads", Delimiter="/")
check("listing", [p["Prefix"] for p in page["CommonPrefixes"]] == ["images/", "rrweb/", "videos/"])

# Presigned URL, fetched by something with no credentials
url = s3.generate_presigned_url("get_object", Params={"Bucket": "uploads", "Key": "images/cat.png"}, ExpiresIn=300)
check("presigned GET", urllib.request.urlopen(url).read() == png)

# Public bucket: plain anonymous HTTP
s3.put_object(Bucket="public", Key="logo.txt", Body=b"hello from objex", ContentType="text/plain")
check("public anonymous GET", urllib.request.urlopen(f"{endpoint}/public/logo.txt").read() == b"hello from objex")

# Browser-facing links must be signed for the address the browser uses.
public = boto3.client("s3", endpoint_url=os.environ["S3_PUBLIC_ENDPOINT"], region_name="auto", config=cfg, **creds)
print("host URL:", public.generate_presigned_url("get_object", Params={"Bucket": "uploads", "Key": "images/cat.png"}, ExpiresIn=600))
print("all checks passed")
