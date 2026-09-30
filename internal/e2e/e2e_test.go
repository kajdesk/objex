// Package e2e drives a real objex server with the official AWS SDK for Go.
package e2e

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/kajdesk/objex/internal/config"
	"github.com/kajdesk/objex/internal/server"
)

const (
	ak   = "OBXTESTACCESSKEY0001"
	sk   = "testsecretkey0000000000000000000000000000"
	roAK = "OBXTESTREADONLY00001"
)

var ctx = context.Background()

type env struct {
	url    string
	s3     *s3.Client
	http   *http.Client
	server *server.Server
}

func start(t *testing.T, useTLS bool) *env {
	t.Helper()
	cfg := config.Default()
	cfg.DataDir = t.TempDir()
	cfg.Fsync = false
	cfg.Keys = []config.Key{{Name: "admin", AccessKey: ak, SecretKey: sk}, {Name: "ro", AccessKey: roAK, SecretKey: sk, ReadOnly: true}}
	srv, err := server.New(cfg, "", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	var ts *httptest.Server
	if useTLS {
		ts = httptest.NewTLSServer(srv.Handler())
	} else {
		ts = httptest.NewServer(srv.Handler())
	}
	t.Cleanup(func() { ts.Close(); _ = srv.Close() })
	hc := ts.Client()
	return &env{url: ts.URL, s3: client(ts.URL, hc, ak, sk), http: hc, server: srv}
}

func client(url string, hc *http.Client, access, secret string) *s3.Client {
	return s3.New(s3.Options{
		BaseEndpoint: aws.String(url), Region: "auto", UsePathStyle: true, HTTPClient: hc,
		Credentials: credentials.NewStaticCredentialsProvider(access, secret, ""),
	})
}

func code(err error) string {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		return ae.ErrorCode()
	}
	if err != nil {
		return err.Error()
	}
	return ""
}

func body(t *testing.T, out *s3.GetObjectOutput, err error) []byte {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	defer out.Body.Close()
	b, err := io.ReadAll(out.Body)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func data(n int, seed byte) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*31) + seed
	}
	return b
}

func mustNil(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func TestBucketsAndObjects(t *testing.T) {
	for _, useTLS := range []bool{false, true} {
		t.Run(fmt.Sprintf("tls=%v", useTLS), func(t *testing.T) {
			e := start(t, useTLS)
			c := e.s3
			_, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("objs")})
			mustNil(t, err)
			_, err = c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("objs")})
			if code(err) != "BucketAlreadyOwnedByYou" {
				t.Fatal(err)
			}
			if _, err := c.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String("objs")}); err != nil {
				t.Fatal(err)
			}
			key := aws.String("dir/hello world+ü.txt")
			put, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("objs"), Key: key, Body: strings.NewReader("hello, world"),
				ContentType: aws.String("text/plain"), Metadata: map[string]string{"color": "blue"}})
			mustNil(t, err)
			if *put.ETag != `"e4d7f1b4ed2e42d15898f4b27b019da4"` {
				t.Fatal(*put.ETag)
			}
			out, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("objs"), Key: key, ChecksumMode: types.ChecksumModeEnabled})
			if string(body(t, out, err)) != "hello, world" || *out.ContentType != "text/plain" || out.Metadata["color"] != "blue" {
				t.Fatalf("%+v", out)
			}
			out, err = c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("objs"), Key: key, Range: aws.String("bytes=7-")})
			if string(body(t, out, err)) != "world" || *out.ContentRange != "bytes 7-11/12" {
				t.Fatal(*out.ContentRange)
			}
			_, err = c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("objs"), Key: key, Range: aws.String("bytes=100-")})
			if code(err) != "InvalidRange" {
				t.Fatal(err)
			}
			_, err = c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("objs"), Key: key, IfNoneMatch: put.ETag})
			var re interface{ HTTPStatusCode() int }
			if !errors.As(err, &re) || re.HTTPStatusCode() != 304 {
				t.Fatalf("If-None-Match: %v", err)
			}
			_, err = c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("objs"), Key: key, Body: strings.NewReader("x"), IfNoneMatch: aws.String("*")})
			if code(err) != "PreconditionFailed" {
				t.Fatal(err)
			}
			// Every checksum algorithm, verified on upload and returned on read.
			for _, a := range []types.ChecksumAlgorithm{types.ChecksumAlgorithmCrc32, types.ChecksumAlgorithmCrc32c, types.ChecksumAlgorithmCrc64nvme, types.ChecksumAlgorithmSha1, types.ChecksumAlgorithmSha256} {
				d := data(300_000, 3)
				_, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("objs"), Key: aws.String("sum"), Body: bytes.NewReader(d), ChecksumAlgorithm: a})
				mustNil(t, err)
				out, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("objs"), Key: aws.String("sum"), ChecksumMode: types.ChecksumModeEnabled})
				if !bytes.Equal(body(t, out, err), d) {
					t.Fatalf("%s mismatch", a)
				}
			}
			// Large streamed object and an empty one.
			big := data(3<<20+17, 9)
			_, err = c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("objs"), Key: aws.String("big"), Body: bytes.NewReader(big)})
			mustNil(t, err)
			out, err = c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("objs"), Key: aws.String("big")})
			if !bytes.Equal(body(t, out, err), big) {
				t.Fatal("big mismatch")
			}
			_, err = c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("objs"), Key: aws.String("empty"), Body: bytes.NewReader(nil)})
			mustNil(t, err)
			_, err = c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("objs"), Key: aws.String("empty"), Range: aws.String("bytes=0-")})
			if code(err) != "InvalidRange" {
				t.Fatalf("empty range: %v", err)
			}
			_, err = c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String("objs"), Key: key})
			mustNil(t, err)
			_, err = c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("objs"), Key: key})
			if code(err) != "NoSuchKey" {
				t.Fatal(err)
			}
			_, err = c.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String("objs")})
			if code(err) != "BucketNotEmpty" {
				t.Fatal(err)
			}
		})
	}
}

func TestListing(t *testing.T) {
	e := start(t, false)
	c := e.s3
	_, _ = c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("lists")})
	keys := []string{"a.txt", "photos/2024/1.jpg", "photos/2024/2.jpg", "photos/2025/1.jpg", "photos/cover.jpg", "z z", "ü"}
	for _, k := range keys {
		_, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("lists"), Key: aws.String(k), Body: strings.NewReader("x")})
		mustNil(t, err)
	}
	var all []string
	p := s3.NewListObjectsV2Paginator(c, &s3.ListObjectsV2Input{Bucket: aws.String("lists"), MaxKeys: aws.Int32(2)})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		mustNil(t, err)
		for _, o := range page.Contents {
			all = append(all, *o.Key)
		}
	}
	if strings.Join(all, ",") != strings.Join(keys, ",") {
		t.Fatal(all)
	}
	// Delimiter listing, paginated one entry at a time.
	var entries []string
	p = s3.NewListObjectsV2Paginator(c, &s3.ListObjectsV2Input{Bucket: aws.String("lists"), Delimiter: aws.String("/"), MaxKeys: aws.Int32(1)})
	for i := 0; p.HasMorePages(); i++ {
		if i > 20 {
			t.Fatal("pagination does not terminate")
		}
		page, err := p.NextPage(ctx)
		mustNil(t, err)
		for _, o := range page.Contents {
			entries = append(entries, *o.Key)
		}
		for _, cp := range page.CommonPrefixes {
			entries = append(entries, *cp.Prefix)
		}
	}
	if strings.Join(entries, ",") != "a.txt,photos/,z z,ü" {
		t.Fatal(entries)
	}
	v1, err := c.ListObjects(ctx, &s3.ListObjectsInput{Bucket: aws.String("lists"), Prefix: aws.String("photos/"), Delimiter: aws.String("/")})
	mustNil(t, err)
	if len(v1.CommonPrefixes) != 2 || len(v1.Contents) != 1 {
		t.Fatalf("%+v", v1)
	}
}

func TestMultipartCopyAndBatchDelete(t *testing.T) {
	e := start(t, true)
	c := e.s3
	_, _ = c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("mpu")})
	for _, algo := range []types.ChecksumAlgorithm{"", types.ChecksumAlgorithmCrc32, types.ChecksumAlgorithmCrc64nvme, types.ChecksumAlgorithmSha256} {
		key := aws.String("video-" + string(algo))
		up, err := c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String("mpu"), Key: key, ContentType: aws.String("video/mp4"), ChecksumAlgorithm: algo})
		mustNil(t, err)
		parts := [][]byte{data(5<<20, 1), data(5<<20, 2), data(1234, 3)}
		var done []types.CompletedPart
		for i, pd := range parts {
			r, err := c.UploadPart(ctx, &s3.UploadPartInput{Bucket: aws.String("mpu"), Key: key, UploadId: up.UploadId, PartNumber: aws.Int32(int32(i + 1)), Body: bytes.NewReader(pd), ChecksumAlgorithm: algo})
			mustNil(t, err)
			done = append(done, types.CompletedPart{PartNumber: aws.Int32(int32(i + 1)), ETag: r.ETag, ChecksumCRC32: r.ChecksumCRC32, ChecksumCRC64NVME: r.ChecksumCRC64NVME, ChecksumSHA256: r.ChecksumSHA256})
		}
		lp, err := c.ListParts(ctx, &s3.ListPartsInput{Bucket: aws.String("mpu"), Key: key, UploadId: up.UploadId})
		if err != nil || len(lp.Parts) != 3 {
			t.Fatal(err)
		}
		out, err := c.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{Bucket: aws.String("mpu"), Key: key, UploadId: up.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: done}})
		mustNil(t, err)
		if !strings.HasSuffix(*out.ETag, `-3"`) {
			t.Fatal(*out.ETag)
		}
		got, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("mpu"), Key: key, ChecksumMode: types.ChecksumModeEnabled})
		if !bytes.Equal(body(t, got, err), bytes.Join(parts, nil)) || *got.ContentType != "video/mp4" {
			t.Fatalf("%s: multipart data mismatch", algo)
		}
		p2, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("mpu"), Key: key, PartNumber: aws.Int32(2)})
		if !bytes.Equal(body(t, p2, err), parts[1]) || *p2.PartsCount != 3 {
			t.Fatal("part 2 mismatch")
		}
	}
	// Abort, and a too-small middle part.
	up, _ := c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String("mpu"), Key: aws.String("small")})
	var small []types.CompletedPart
	for n := int32(1); n <= 2; n++ {
		r, err := c.UploadPart(ctx, &s3.UploadPartInput{Bucket: aws.String("mpu"), Key: aws.String("small"), UploadId: up.UploadId, PartNumber: aws.Int32(n), Body: strings.NewReader("tiny")})
		mustNil(t, err)
		small = append(small, types.CompletedPart{PartNumber: aws.Int32(n), ETag: r.ETag})
	}
	_, err := c.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{Bucket: aws.String("mpu"), Key: aws.String("small"), UploadId: up.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: small}})
	if code(err) != "EntityTooSmall" {
		t.Fatal(err)
	}
	_, err = c.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: aws.String("mpu"), Key: aws.String("small"), UploadId: up.UploadId})
	mustNil(t, err)

	// CopyObject (keep and replace metadata) and UploadPartCopy with a range.
	_, err = c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("mpu"), Key: aws.String("orig file"), Body: bytes.NewReader(data(10_000, 5)), Metadata: map[string]string{"k": "v"}})
	mustNil(t, err)
	_, err = c.CopyObject(ctx, &s3.CopyObjectInput{Bucket: aws.String("mpu"), Key: aws.String("copy"), CopySource: aws.String("mpu/orig%20file")})
	mustNil(t, err)
	got, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("mpu"), Key: aws.String("copy")})
	if !bytes.Equal(body(t, got, err), data(10_000, 5)) || got.Metadata["k"] != "v" {
		t.Fatal("copy mismatch")
	}
	_, err = c.CopyObject(ctx, &s3.CopyObjectInput{Bucket: aws.String("mpu"), Key: aws.String("copy"), CopySource: aws.String("mpu/copy"), MetadataDirective: types.MetadataDirectiveReplace, ContentType: aws.String("image/png")})
	mustNil(t, err)
	head, _ := c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String("mpu"), Key: aws.String("copy")})
	if *head.ContentType != "image/png" || len(head.Metadata) != 0 {
		t.Fatalf("%+v", head)
	}
	up, _ = c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String("mpu"), Key: aws.String("assembled")})
	pc, err := c.UploadPartCopy(ctx, &s3.UploadPartCopyInput{Bucket: aws.String("mpu"), Key: aws.String("assembled"), UploadId: up.UploadId, PartNumber: aws.Int32(1), CopySource: aws.String("mpu/orig%20file"), CopySourceRange: aws.String("bytes=0-99")})
	mustNil(t, err)
	_, err = c.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{Bucket: aws.String("mpu"), Key: aws.String("assembled"), UploadId: up.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{PartNumber: aws.Int32(1), ETag: pc.CopyPartResult.ETag}}}})
	mustNil(t, err)
	got, err = c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("mpu"), Key: aws.String("assembled")})
	if !bytes.Equal(body(t, got, err), data(10_000, 5)[:100]) {
		t.Fatal("part copy mismatch")
	}
	// DeleteObjects.
	var ids []types.ObjectIdentifier
	for _, k := range []string{"copy", "assembled", "missing"} {
		ids = append(ids, types.ObjectIdentifier{Key: aws.String(k)})
	}
	del, err := c.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: aws.String("mpu"), Delete: &types.Delete{Objects: ids}})
	if err != nil || len(del.Deleted) != 3 {
		t.Fatal(err)
	}
}

func TestAccessControlAndCORS(t *testing.T) {
	e := start(t, false)
	c := e.s3
	_, _ = c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("private-b")})
	_, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("private-b"), Key: aws.String("secret.txt"), Body: strings.NewReader("s3cr3t")})
	mustNil(t, err)
	get := func(url string) (int, string) {
		r, err := e.http.Get(url)
		mustNil(t, err)
		defer r.Body.Close()
		b, _ := io.ReadAll(r.Body)
		return r.StatusCode, string(b)
	}
	if st, b := get(e.url + "/_objex/health"); st != 200 || b != "ok\n" {
		t.Fatal(st, b)
	}
	if st, b := get(e.url + "/private-b/secret.txt"); st != 403 || !strings.Contains(b, "AccessDenied") {
		t.Fatal(st, b)
	}
	if _, err := client(e.url, e.http, ak, "wrong").ListBuckets(ctx, &s3.ListBucketsInput{}); code(err) != "SignatureDoesNotMatch" {
		t.Fatal(err)
	}
	if _, err := client(e.url, e.http, "OBXNOSUCHKEY", sk).ListBuckets(ctx, &s3.ListBucketsInput{}); code(err) != "InvalidAccessKeyId" {
		t.Fatal(err)
	}
	pre := s3.NewPresignClient(c)
	pg, err := pre.PresignGetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("private-b"), Key: aws.String("secret.txt")}, s3.WithPresignExpires(5*time.Minute))
	mustNil(t, err)
	if st, b := get(pg.URL); st != 200 || b != "s3cr3t" {
		t.Fatal(st, b)
	}
	pp, err := pre.PresignPutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("private-b"), Key: aws.String("via-presign")}, s3.WithPresignExpires(5*time.Minute))
	mustNil(t, err)
	req, _ := http.NewRequest(http.MethodPut, pp.URL, strings.NewReader("uploaded"))
	r, err := e.http.Do(req)
	if err != nil || r.StatusCode != 200 {
		t.Fatal(err, r.StatusCode)
	}
	ro := client(e.url, e.http, roAK, sk)
	if _, err := ro.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("private-b"), Key: aws.String("via-presign")}); err != nil {
		t.Fatal(err)
	}
	if _, err := ro.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("private-b"), Key: aws.String("x"), Body: strings.NewReader("x")}); code(err) != "AccessDenied" {
		t.Fatal(err)
	}
	// Public-read buckets and ACL toggling.
	_, err = c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("public-b"), ACL: types.BucketCannedACLPublicRead})
	mustNil(t, err)
	_, _ = c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("public-b"), Key: aws.String("hi.txt"), Body: strings.NewReader("hi")})
	if st, b := get(e.url + "/public-b/hi.txt"); st != 200 || b != "hi" {
		t.Fatal(st, b)
	}
	acl, err := c.GetBucketAcl(ctx, &s3.GetBucketAclInput{Bucket: aws.String("public-b")})
	if err != nil || len(acl.Grants) != 2 {
		t.Fatal(err)
	}
	_, err = c.PutBucketAcl(ctx, &s3.PutBucketAclInput{Bucket: aws.String("public-b"), ACL: types.BucketCannedACLPrivate})
	mustNil(t, err)
	if st, _ := get(e.url + "/public-b/hi.txt"); st != 403 {
		t.Fatal(st)
	}
	// CORS.
	_, err = c.PutBucketCors(ctx, &s3.PutBucketCorsInput{Bucket: aws.String("public-b"), CORSConfiguration: &types.CORSConfiguration{CORSRules: []types.CORSRule{{
		AllowedOrigins: []string{"https://app.example.com"}, AllowedMethods: []string{"GET", "PUT"}, AllowedHeaders: []string{"*"}, ExposeHeaders: []string{"ETag"}, MaxAgeSeconds: aws.Int32(600),
	}}}})
	mustNil(t, err)
	cors, err := c.GetBucketCors(ctx, &s3.GetBucketCorsInput{Bucket: aws.String("public-b")})
	if err != nil || len(cors.CORSRules) != 1 {
		t.Fatal(err)
	}
	pf, _ := http.NewRequest(http.MethodOptions, e.url+"/public-b/file.txt", nil)
	pf.Header.Set("Origin", "https://app.example.com")
	pf.Header.Set("Access-Control-Request-Method", "PUT")
	pf.Header.Set("Access-Control-Request-Headers", "content-type, x-amz-date")
	r, err = e.http.Do(pf)
	if err != nil || r.StatusCode != 200 || r.Header.Get("Access-Control-Allow-Origin") != "https://app.example.com" || r.Header.Get("Access-Control-Max-Age") != "600" {
		t.Fatalf("preflight: %v %v %v", err, r.StatusCode, r.Header)
	}
	pf.Header.Set("Origin", "https://evil.example")
	if r, _ = e.http.Do(pf); r.StatusCode != 403 {
		t.Fatal(r.StatusCode)
	}
	pg, _ = pre.PresignGetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("public-b"), Key: aws.String("hi.txt")})
	gr, _ := http.NewRequest(http.MethodGet, pg.URL, nil)
	gr.Header.Set("Origin", "https://app.example.com")
	if r, _ = e.http.Do(gr); r.Header.Get("Access-Control-Allow-Origin") != "https://app.example.com" || r.Header.Get("Access-Control-Expose-Headers") != "ETag" {
		t.Fatalf("cors on GET: %v", r.Header)
	}
}

// Keep the TLS client happy with the test certificate when constructing one by hand.
var _ = tls.Config{}
