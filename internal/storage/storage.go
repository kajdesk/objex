// Package storage defines the storage contract the S3 layer depends on. The
// single-node implementation lives in storage/local; errors are s3err values.
package storage

import (
	"context"
	"io"
	"strings"
	"time"

	"github.com/kajdesk/objex/internal/checksum"
	"github.com/kajdesk/objex/internal/s3err"
)

const (
	MaxObjectSize = 5 << 40 // 5 TiB
	MaxPutSize    = 5 << 30 // 5 GiB, also the largest part
	MinPartSize   = 5 << 20 // 5 MiB, for every part but the last
	MaxPartNumber = 10000
	MaxKeyLength  = 1024
	MaxUserMeta   = 2048
)

type CORSRule struct {
	ID             string   `json:"id,omitempty"`
	AllowedOrigins []string `json:"allowed_origins"`
	AllowedMethods []string `json:"allowed_methods"`
	AllowedHeaders []string `json:"allowed_headers,omitempty"`
	ExposeHeaders  []string `json:"expose_headers,omitempty"`
	MaxAgeSeconds  int      `json:"max_age_seconds,omitempty"`
}

type Bucket struct {
	Name       string     `json:"name"`
	Created    time.Time  `json:"created"`
	PublicRead bool       `json:"public_read,omitempty"`
	CORS       []CORSRule `json:"cors,omitempty"`
}

// Metadata is the standard and user metadata stored with an object.
type Metadata struct {
	ContentType        string `json:"content_type,omitempty"`
	ContentEncoding    string `json:"content_encoding,omitempty"`
	ContentDisposition string `json:"content_disposition,omitempty"`
	ContentLanguage    string `json:"content_language,omitempty"`
	CacheControl       string `json:"cache_control,omitempty"`
	Expires            string `json:"expires,omitempty"`
	// User holds x-amz-meta-* values, names lowercased without the prefix.
	User map[string]string `json:"user,omitempty"`
}

type Object struct {
	Key  string
	Size int64
	// ETag is hex MD5, or "hex-N" for multipart objects, without quotes.
	ETag         string
	LastModified time.Time
	Metadata     Metadata
	Checksum     *checksum.Checksum
	// Parts holds part sizes for multipart objects; nil otherwise.
	Parts []int64
}

// Expect describes what an upload must match and which checksum to compute.
type Expect struct {
	ContentMD5 []byte
	SHA256     []byte
	// Checksum is a value supplied in a header.
	Checksum *checksum.Checksum
	// Algo is the algorithm to compute when the value arrives in a trailer, or
	// not at all.
	Algo checksum.Algo
	// Size is the declared length, or -1 when unknown.
	Size int64
	// Trailer returns a checksum that arrived after the body (aws-chunked).
	Trailer func() (checksum.Checksum, bool)
}

// WriteConditions are If-Match / If-None-Match on writes.
type WriteConditions struct {
	IfMatch     string
	IfNoneMatch string
}

// Check evaluates the conditions against the current ETag (nil: no object).
func (c WriteConditions) Check(current *string) error {
	if c.IfMatch != "" {
		if current == nil {
			return s3err.NoSuchKey
		}
		if !ETagMatches(c.IfMatch, *current) {
			return s3err.PreconditionFailed
		}
	}
	if c.IfNoneMatch != "" && current != nil && ETagMatches(c.IfNoneMatch, *current) {
		return s3err.PreconditionFailed
	}
	return nil
}

// ReadConditions are the conditional GET/HEAD headers (or x-amz-copy-source-if-*).
type ReadConditions struct {
	IfMatch, IfNoneMatch               string
	IfModifiedSince, IfUnmodifiedSince time.Time
}

// Check evaluates the conditions per RFC 7232 as S3 does. For copy sources a
// "not modified" outcome is PreconditionFailed instead.
func (c ReadConditions) Check(o Object, copySource bool) error {
	modified := o.LastModified.Truncate(time.Second)
	if c.IfMatch != "" {
		if !ETagMatches(c.IfMatch, o.ETag) {
			return s3err.PreconditionFailed
		}
	} else if !c.IfUnmodifiedSince.IsZero() && modified.After(c.IfUnmodifiedSince) {
		return s3err.PreconditionFailed
	}
	notModified := s3err.NotModified
	if copySource {
		notModified = s3err.PreconditionFailed
	}
	if c.IfNoneMatch != "" {
		if ETagMatches(c.IfNoneMatch, o.ETag) {
			return notModified
		}
	} else if !c.IfModifiedSince.IsZero() && !modified.After(c.IfModifiedSince) {
		return notModified
	}
	return nil
}

// ETagMatches matches an If-Match / If-None-Match value against an ETag.
func ETagMatches(header, etag string) bool {
	for _, t := range strings.Split(header, ",") {
		t = strings.TrimSpace(t)
		if t == "*" || strings.Trim(strings.TrimPrefix(t, "W/"), "\"") == etag {
			return true
		}
	}
	return false
}

type PutOptions struct {
	Metadata Metadata
	Expect   Expect
	Cond     WriteConditions
}

type CopyOptions struct {
	// ReplaceMetadata, when non-nil, replaces the source metadata.
	ReplaceMetadata *Metadata
	SourceCond      ReadConditions
	Cond            WriteConditions
}

type ListOptions struct {
	Prefix    string
	Delimiter string
	// Marker is an exclusive start key.
	Marker string
	Limit  int
}

type ListResult struct {
	Objects   []Object
	Prefixes  []string
	Truncated bool
	// NextMarker is the last key or common prefix returned, when truncated.
	NextMarker string
}

type Part struct {
	Number       int
	ETag         string
	Size         int64
	LastModified time.Time
	Checksum     *checksum.Checksum
}

type Upload struct {
	Key, UploadID string
	Initiated     time.Time
	ChecksumAlgo  checksum.Algo
	ChecksumType  checksum.Type
}

type ListPartsResult struct {
	Upload     Upload
	Parts      []Part
	Truncated  bool
	NextMarker int
}

type ListUploadsOptions struct {
	Prefix, Delimiter, KeyMarker, UploadIDMarker string
	Limit                                        int
}

type ListUploadsResult struct {
	Uploads                           []Upload
	Prefixes                          []string
	Truncated                         bool
	NextKeyMarker, NextUploadIDMarker string
}

type CompletePart struct {
	Number   int
	ETag     string
	Checksum *checksum.Checksum
}

type CompleteOptions struct {
	Cond WriteConditions
	// Checksum is a full-object checksum supplied by the client.
	Checksum *checksum.Checksum
}

// ObjectReader is an opened object. Holding it keeps the data readable even if
// the object is overwritten or deleted meanwhile.
type ObjectReader interface {
	// Check opens the data holding byte start, so a missing or truncated blob
	// fails the request before any response headers are sent.
	Check(start int64) error
	// CopyRange writes bytes [start, start+length) to w. Segments copied in full
	// are verified against their stored checksum; the final bytes of a segment
	// are only written once it verifies, so corrupt data never arrives complete.
	CopyRange(w io.Writer, start, length int64) error
	// PartRange is the byte range of part n (1-based) of a multipart object.
	PartRange(n int) (start, length int64, ok bool)
	Close() error
}

// Store is the object storage contract.
type Store interface {
	ListBuckets(ctx context.Context) ([]Bucket, error)
	CreateBucket(ctx context.Context, name string, publicRead bool) error
	GetBucket(ctx context.Context, name string) (Bucket, error)
	UpdateBucket(ctx context.Context, name string, update func(*Bucket)) error
	DeleteBucket(ctx context.Context, name string) error

	PutObject(ctx context.Context, bucket, key string, body io.Reader, opts PutOptions) (Object, error)
	HeadObject(ctx context.Context, bucket, key string) (Object, error)
	OpenObject(ctx context.Context, bucket, key string) (Object, ObjectReader, error)
	// DeleteObjects deletes keys, returning one error (or nil) per key.
	DeleteObjects(ctx context.Context, bucket string, keys []string) ([]error, error)
	CopyObject(ctx context.Context, srcBucket, srcKey, bucket, key string, opts CopyOptions) (Object, error)
	ListObjects(ctx context.Context, bucket string, opts ListOptions) (ListResult, error)

	CreateMultipart(ctx context.Context, bucket, key string, meta Metadata, algo checksum.Algo, typ checksum.Type) (string, error)
	UploadPart(ctx context.Context, bucket, key, uploadID string, n int, body io.Reader, expect Expect) (Part, error)
	// UploadPartCopy copies src (or the inclusive byte range rng of it) into a part.
	UploadPartCopy(ctx context.Context, srcBucket, srcKey string, rng *[2]int64, cond ReadConditions, bucket, key, uploadID string, n int) (Part, error)
	CompleteMultipart(ctx context.Context, bucket, key, uploadID string, parts []CompletePart, opts CompleteOptions) (Object, error)
	AbortMultipart(ctx context.Context, bucket, key, uploadID string) error
	ListParts(ctx context.Context, bucket, key, uploadID string, marker, limit int) (ListPartsResult, error)
	ListUploads(ctx context.Context, bucket string, opts ListUploadsOptions) (ListUploadsResult, error)

	Close() error
}
