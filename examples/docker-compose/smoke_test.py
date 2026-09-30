"""End-to-end check of objex from another container, the way an app would use it."""

import gzip
import json
import os
import urllib.request

import boto3
from botocore.config import Config

endpoint = os.environ["S3_ENDPOINT"]
creds = dict(aws_access_key_id=os.environ["S3_ACCESS_KEY"], aws_secret_access_key=os.environ["S3_SECRET_KEY"])
# Path-style addressing: "bucket.objex" does not resolve inside a compose network.
cfg = Config(
    s3={"addressing_style": "path"},
    signature_version="s3v4",
    request_checksum_calculation="when_required",
    response_checksum_validation="when_required",
)
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

# Range reads
part = s3.get_object(Bucket="uploads", Key="images/cat.png", Range="bytes=1000-1999")["Body"].read()
check("range read", part == png[1000:2000])

# Listing like a file browser
page = s3.list_objects_v2(Bucket="uploads", Delimiter="/")
check("listing", [p["Prefix"] for p in page["CommonPrefixes"]] == ["images/", "rrweb/"])

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
