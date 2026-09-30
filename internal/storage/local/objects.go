package local

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"strings"
	"time"

	"github.com/kajdesk/objex/internal/s3err"
	"github.com/kajdesk/objex/internal/storage"
	bolt "go.etcd.io/bbolt"
)

func now() int64 { return time.Now().UTC().Truncate(time.Millisecond).UnixNano() }

func (s *Store) bucketExists(bucket string) error {
	return s.view(func(tx *bolt.Tx) error { return requireBucket(tx, bucket) })
}

// commitObject publishes rec as bucket/key subject to cond, then queues the
// replaced object's blobs for reclamation. The caller still owns rec's blobs
// if it fails.
func (s *Store) commitObject(bucket, key string, rec *objectRecord, cond storage.WriteConditions) error {
	var garbage []segment
	err := s.update(func(tx *bolt.Tx) error {
		garbage = nil
		if err := requireBucket(tx, bucket); err != nil {
			return err
		}
		old, err := getObject(tx, bucket, key)
		if err != nil {
			return err
		}
		var etag *string
		if old != nil {
			etag = &old.ETag
		}
		if err := cond.Check(etag); err != nil {
			return err
		}
		if old != nil {
			garbage = old.Segments
			if err := dropRefs(tx, old.Segments); err != nil {
				return err
			}
		}
		if err := addRefs(tx, rec.Segments); err != nil {
			return err
		}
		return tx.Bucket(objectsTable).Put(objectKey(bucket, key), enc(rec))
	})
	if err == nil {
		s.reclaim.queue(garbage)
	}
	return err
}

func (s *Store) PutObject(ctx context.Context, bucket, key string, body io.Reader, opts storage.PutOptions) (storage.Object, error) {
	if err := validateKey(key); err != nil {
		return storage.Object{}, err
	}
	if err := s.bucketExists(bucket); err != nil {
		return storage.Object{}, err
	}
	if opts.Expect.Size > storage.MaxPutSize {
		return storage.Object{}, s3err.EntityTooLarge
	}
	d, err := s.writeBlob(ctx, body, opts.Expect, storage.MaxPutSize)
	if err != nil {
		return storage.Object{}, err
	}
	rec := &objectRecord{
		Size: d.seg.Size, ETag: hex.EncodeToString(d.md5), Modified: now(),
		Meta: opts.Metadata, Checksum: d.checksum, Segments: []segment{d.seg},
	}
	if err := s.commitObject(bucket, key, rec, opts.Cond); err != nil {
		_ = s.removeBlob(d.seg.Blob)
		return storage.Object{}, err
	}
	return rec.object(key), nil
}

func (s *Store) HeadObject(_ context.Context, bucket, key string) (storage.Object, error) {
	var out storage.Object
	err := s.view(func(tx *bolt.Tx) error {
		if err := requireBucket(tx, bucket); err != nil {
			return err
		}
		rec, err := getObject(tx, bucket, key)
		if err != nil {
			return err
		}
		if rec == nil {
			return s3err.NoSuchKey
		}
		out = rec.object(key)
		return nil
	})
	return out, err
}

// openRecord reads an object's record and leases its blobs (see leases).
func (s *Store) openRecord(bucket, key string) (*objectRecord, *objectReader, error) {
	s.leases.gate.RLock()
	defer s.leases.gate.RUnlock()
	var rec *objectRecord
	err := s.view(func(tx *bolt.Tx) error {
		if err := requireBucket(tx, bucket); err != nil {
			return err
		}
		var err error
		rec, err = getObject(tx, bucket, key)
		if err == nil && rec == nil {
			err = s3err.NoSuchKey
		}
		return err
	})
	if err != nil {
		return nil, nil, err
	}
	r := newObjectReader(s, rec.Segments)
	s.leases.acquire(r.ids)
	return rec, r, nil
}

func (s *Store) OpenObject(_ context.Context, bucket, key string) (storage.Object, storage.ObjectReader, error) {
	rec, r, err := s.openRecord(bucket, key)
	if err != nil {
		return storage.Object{}, nil, err
	}
	return rec.object(key), r, nil
}

func (s *Store) DeleteObjects(_ context.Context, bucket string, keys []string) ([]error, error) {
	var results []error
	var garbage []segment
	err := s.update(func(tx *bolt.Tx) error {
		results, garbage = make([]error, len(keys)), nil
		if err := requireBucket(tx, bucket); err != nil {
			return err
		}
		t := tx.Bucket(objectsTable)
		for i, k := range keys {
			if err := validateKey(k); err != nil {
				results[i] = err
				continue
			}
			rec, err := getObject(tx, bucket, k)
			if err != nil {
				return err
			}
			if rec == nil {
				continue // deleting a missing key succeeds, as in S3
			}
			if err := dropRefs(tx, rec.Segments); err != nil {
				return err
			}
			if err := t.Delete(objectKey(bucket, k)); err != nil {
				return err
			}
			garbage = append(garbage, rec.Segments...)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	s.reclaim.queue(garbage)
	return results, nil
}

func (s *Store) CopyObject(_ context.Context, srcBucket, srcKey, bucket, key string, opts storage.CopyOptions) (storage.Object, error) {
	if err := validateKey(key); err != nil {
		return storage.Object{}, err
	}
	if srcBucket == bucket && srcKey == key && opts.ReplaceMetadata == nil {
		return storage.Object{}, s3err.InvalidRequest.WithMessage("This copy request is illegal because it is trying to copy an object to itself without changing the object's metadata, storage class, website redirect location or encryption attributes.")
	}
	if err := s.bucketExists(bucket); err != nil {
		return storage.Object{}, err
	}
	src, reader, err := s.openRecord(srcBucket, srcKey)
	if err != nil {
		return storage.Object{}, err
	}
	defer reader.Close()
	if err := opts.SourceCond.Check(src.object(srcKey), true); err != nil {
		return storage.Object{}, err
	}
	// Hard-link every segment: copies take no time and no space on one node.
	segs := make([]segment, 0, len(src.Segments))
	undo := func() {
		for _, sg := range segs {
			_ = s.removeBlob(sg.Blob)
		}
	}
	for _, sg := range src.Segments {
		dup, err := s.duplicate(sg)
		if err != nil {
			undo()
			if errors.Is(err, os.ErrNotExist) {
				return storage.Object{}, s3err.Internal(integrityError(sg.Blob, "blob file is missing"))
			}
			return storage.Object{}, err
		}
		segs = append(segs, dup)
	}
	rec := *src
	rec.Segments, rec.Modified = segs, now()
	if opts.ReplaceMetadata != nil {
		rec.Meta = *opts.ReplaceMetadata
	}
	if err := s.commitObject(bucket, key, &rec, opts.Cond); err != nil {
		undo()
		return storage.Object{}, err
	}
	return rec.object(key), nil
}

// commonPrefix returns key's common prefix (after prefix, up to and including
// the delimiter), if any.
func commonPrefix(key, prefix, delim string) (string, bool) {
	if delim == "" {
		return "", false
	}
	i := strings.Index(key[len(prefix):], delim)
	if i < 0 {
		return "", false
	}
	return key[:len(prefix)+i+len(delim)], true
}

func (s *Store) ListObjects(_ context.Context, bucket string, o storage.ListOptions) (storage.ListResult, error) {
	var res storage.ListResult
	err := s.view(func(tx *bolt.Tx) error {
		if err := requireBucket(tx, bucket); err != nil {
			return err
		}
		if o.Limit <= 0 {
			return nil
		}
		base := objectPrefix(bucket)
		c := tx.Bucket(objectsTable).Cursor()
		start := objectKey(bucket, o.Prefix)
		if o.Marker >= o.Prefix {
			start = objectKey(bucket, o.Marker)
		}
		count := 0
		for k, v := c.Seek(start); k != nil && bytes.HasPrefix(k, base); {
			key := string(k[len(base):])
			if !strings.HasPrefix(key, o.Prefix) {
				break
			}
			if key <= o.Marker {
				k, v = c.Next()
				continue
			}
			if cp, ok := commonPrefix(key, o.Prefix, o.Delimiter); ok {
				if cp > o.Marker {
					if count == o.Limit {
						res.Truncated = true
						break
					}
					res.Prefixes = append(res.Prefixes, cp)
					res.NextMarker = cp
					count++
				}
				// Skip every key under this prefix in one seek.
				next := successor(objectKey(bucket, cp))
				if next == nil {
					break
				}
				k, v = c.Seek(next)
				continue
			}
			if count == o.Limit {
				res.Truncated = true
				break
			}
			rec, err := dec[objectRecord](v)
			if err != nil {
				return err
			}
			res.Objects = append(res.Objects, rec.object(key))
			res.NextMarker = key
			count++
			k, v = c.Next()
		}
		if !res.Truncated {
			res.NextMarker = ""
		}
		return nil
	})
	return res, err
}
