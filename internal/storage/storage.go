package storage

import (
	"context"
	"errors"
	"io"
	"time"
)

var (
	ErrNoSuchBucket       = errors.New("bucket does not exist")
	ErrBucketExists       = errors.New("bucket already exists")
	ErrBucketNotEmpty     = errors.New("bucket is not empty")
	ErrNoSuchKey          = errors.New("object does not exist")
	ErrPreconditionFailed = errors.New("precondition failed")
	ErrEntityTooLarge     = errors.New("object is too large")
)

type Bucket struct {
	Name       string    `json:"name"`
	Created    time.Time `json:"created"`
	PublicRead bool      `json:"public_read,omitempty"`
}

type Metadata struct {
	ContentType        string            `json:"content_type,omitempty"`
	ContentEncoding    string            `json:"content_encoding,omitempty"`
	ContentDisposition string            `json:"content_disposition,omitempty"`
	ContentLanguage    string            `json:"content_language,omitempty"`
	CacheControl       string            `json:"cache_control,omitempty"`
	Expires            string            `json:"expires,omitempty"`
	User               map[string]string `json:"user,omitempty"`
}

type Object struct {
	Bucket       string    `json:"-"`
	Key          string    `json:"key"`
	Blob         string    `json:"blob"`
	Size         int64     `json:"size"`
	ETag         string    `json:"etag"`
	CRC32C       uint32    `json:"crc32c"`
	LastModified time.Time `json:"last_modified"`
	Metadata     Metadata  `json:"metadata,omitempty"`
}

type PutOptions struct {
	Metadata    Metadata
	MaxSize     int64
	SHA256      []byte
	IfMatch     string
	IfNoneMatch string
}

type ListOptions struct {
	Prefix    string
	Delimiter string
	Marker    string
	Limit     int
}

type ListResult struct {
	Objects    []Object
	Prefixes   []string
	Truncated  bool
	NextMarker string
}

type Store interface {
	Close() error
	ListBuckets(context.Context) ([]Bucket, error)
	CreateBucket(context.Context, string, bool) error
	GetBucket(context.Context, string) (Bucket, error)
	DeleteBucket(context.Context, string) error
	PutObject(context.Context, string, string, io.Reader, PutOptions) (Object, error)
	HeadObject(context.Context, string, string) (Object, error)
	OpenObject(context.Context, string, string) (Object, ReadSeekCloser, error)
	DeleteObject(context.Context, string, string) error
	ListObjects(context.Context, string, ListOptions) (ListResult, error)
}

// osFile is the minimal seekable/closable reader needed by the HTTP layer.
// *os.File satisfies it; the private interface keeps storage callers decoupled.
type ReadSeekCloser interface {
	io.Reader
	io.ReaderAt
	io.Seeker
	io.Closer
}
