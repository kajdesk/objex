package local

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"time"

	"github.com/kajdesk/objex/internal/checksum"
	"github.com/kajdesk/objex/internal/s3err"
	"github.com/kajdesk/objex/internal/storage"
	bolt "go.etcd.io/bbolt"
)

// Tables (bbolt buckets).
var (
	bucketsTable = []byte("buckets") // name -> storage.Bucket
	objectsTable = []byte("objects") // bucket \0 key -> objectRecord
	uploadsTable = []byte("uploads") // bucket \0 key \0 uploadID -> uploadRecord
	partsTable   = []byte("parts")   // uploadID + uint32 part (BE) -> partRecord
	refsTable    = []byte("blobrefs")
	allTables    = [][]byte{bucketsTable, objectsTable, uploadsTable, partsTable, refsTable}
)

// segment is one immutable blob of an object's data.
type segment struct {
	Blob string `json:"b"`
	Size int64  `json:"s"`
	// CRC32C is the internal integrity checksum of the blob.
	CRC32C uint32 `json:"c"`
}

type objectRecord struct {
	Size      int64              `json:"size"`
	ETag      string             `json:"etag"`
	Modified  int64              `json:"mtime"` // unix nanoseconds
	Meta      storage.Metadata   `json:"meta"`
	Checksum  *checksum.Checksum `json:"checksum,omitempty"`
	Multipart bool               `json:"mp,omitempty"`
	Segments  []segment          `json:"segs"`
}

func (r *objectRecord) object(key string) storage.Object {
	o := storage.Object{
		Key: key, Size: r.Size, ETag: r.ETag, LastModified: time.Unix(0, r.Modified).UTC(),
		Metadata: r.Meta, Checksum: r.Checksum,
	}
	if r.Multipart {
		o.Parts = make([]int64, len(r.Segments))
		for i, s := range r.Segments {
			o.Parts[i] = s.Size
		}
	}
	return o
}

type uploadRecord struct {
	Initiated int64            `json:"initiated"`
	Meta      storage.Metadata `json:"meta"`
	Algo      checksum.Algo    `json:"algo,omitempty"`
	Type      checksum.Type    `json:"type,omitempty"`
}

type partRecord struct {
	ETag     string             `json:"etag"`
	Size     int64              `json:"size"`
	Modified int64              `json:"mtime"`
	Checksum *checksum.Checksum `json:"checksum,omitempty"`
	Blob     segment            `json:"blob"`
}

func (p *partRecord) part(n int) storage.Part {
	return storage.Part{Number: n, ETag: p.ETag, Size: p.Size, LastModified: time.Unix(0, p.Modified).UTC(), Checksum: p.Checksum}
}

func enc(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err) // records are plain structs; this cannot fail
	}
	return b
}

func dec[T any](b []byte) (T, error) {
	var v T
	if err := json.Unmarshal(b, &v); err != nil {
		return v, s3err.Internal(err)
	}
	return v, nil
}

// Key layouts. Bucket names never contain \0, so prefixes cannot collide.
func objectPrefix(bucket string) []byte { return append([]byte(bucket), 0) }
func objectKey(bucket, key string) []byte {
	return append(objectPrefix(bucket), key...)
}
func uploadKey(bucket, key, id string) []byte {
	k := objectKey(bucket, key)
	k = append(k, 0)
	return append(k, id...)
}
func partKey(uploadID string, n int) []byte {
	k := make([]byte, len(uploadID)+4)
	copy(k, uploadID)
	binary.BigEndian.PutUint32(k[len(uploadID):], uint32(n))
	return k
}

// successor returns the smallest key greater than every key with prefix p.
func successor(p []byte) []byte {
	s := bytes.Clone(p)
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] < 0xff {
			s[i]++
			return s[:i+1]
		}
	}
	return nil // no successor: p is all 0xff
}

func requireBucket(tx *bolt.Tx, name string) error {
	if tx.Bucket(bucketsTable).Get([]byte(name)) == nil {
		return s3err.NoSuchBucket
	}
	return nil
}

func getObject(tx *bolt.Tx, bucket, key string) (*objectRecord, error) {
	v := tx.Bucket(objectsTable).Get(objectKey(bucket, key))
	if v == nil {
		return nil, nil
	}
	r, err := dec[objectRecord](v)
	return &r, err
}

func addRefs(tx *bolt.Tx, segs []segment) error {
	t := tx.Bucket(refsTable)
	for _, s := range segs {
		if err := t.Put([]byte(s.Blob), nil); err != nil {
			return err
		}
	}
	return nil
}

func dropRefs(tx *bolt.Tx, segs []segment) error {
	t := tx.Bucket(refsTable)
	for _, s := range segs {
		if err := t.Delete([]byte(s.Blob)); err != nil {
			return err
		}
	}
	return nil
}

// removeParts deletes every part of an upload, returning their blobs.
func removeParts(tx *bolt.Tx, uploadID string) ([]segment, error) {
	t := tx.Bucket(partsTable)
	prefix := []byte(uploadID)
	var blobs []segment
	var keys [][]byte
	c := t.Cursor()
	for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
		p, err := dec[partRecord](v)
		if err != nil {
			return nil, err
		}
		blobs = append(blobs, p.Blob)
		keys = append(keys, bytes.Clone(k))
	}
	for _, k := range keys {
		if err := t.Delete(k); err != nil {
			return nil, err
		}
	}
	return blobs, nil
}

// update runs fn in a write transaction and returns once it is committed.
//
// With fsync, concurrent writes are group-committed: one goroutine takes every
// write waiting whenever it is free and commits them in a single transaction,
// so many uploads share one flush. (bbolt's Batch starts a new batch every few
// milliseconds instead, and each pays its own flush.) A client error from one
// fn (a failed precondition, a missing bucket...) fails only that write, so fn
// must return such errors before modifying anything; an internal error rolls
// back the whole batch.
func (s *Store) update(fn func(*bolt.Tx) error) error {
	if !s.opts.Fsync {
		return s3err.Internal(s.db.Update(fn))
	}
	j := &commitJob{fn: fn, done: make(chan error, 1)}
	select {
	case s.commits <- j:
	case <-s.stopCommits:
		return s3err.Internal(errors.New("store is closed"))
	}
	return <-j.done
}

type commitJob struct {
	fn   func(*bolt.Tx) error
	done chan error
}

const maxCommitBatch = 1024

func isClientError(err error) bool {
	var e *s3err.Error
	return errors.As(err, &e) && e.Status < 500
}

func (s *Store) committer() {
	defer close(s.committerDone)
	for {
		var first *commitJob
		select {
		case first = <-s.commits:
		case <-s.stopCommits:
			return
		}
		batch := []*commitJob{first}
	drain:
		for len(batch) < maxCommitBatch {
			select {
			case j := <-s.commits:
				batch = append(batch, j)
			default:
				break drain
			}
		}
		results := make([]error, len(batch))
		err := s.db.Update(func(tx *bolt.Tx) error {
			for i, j := range batch {
				if e := j.fn(tx); e != nil {
					if !isClientError(e) {
						return e
					}
					results[i] = e
				}
			}
			return nil
		})
		for i, j := range batch {
			if err != nil {
				results[i] = s3err.Internal(err)
			}
			j.done <- results[i]
		}
	}
}

func (s *Store) view(fn func(*bolt.Tx) error) error {
	return s3err.Internal(s.db.View(fn))
}
