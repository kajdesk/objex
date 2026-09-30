package local

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/kajdesk/objex/internal/storage"
	bolt "go.etcd.io/bbolt"
)

var (
	bucketsTable = []byte("buckets")
	objectsTable = []byte("objects")
	castagnoli   = crc32.MakeTable(crc32.Castagnoli)
)

type Store struct {
	root    string
	db      *bolt.DB
	fsync   bool
	reclaim chan string
	closed  chan struct{}
	wg      sync.WaitGroup
}

func Open(root string, fsync bool) (*Store, error) {
	if err := os.MkdirAll(root, 0o750); err != nil {
		return nil, fmt.Errorf("create data directory: %w", err)
	}
	db, err := bolt.Open(filepath.Join(root, "meta.db"), 0o600, &bolt.Options{Timeout: 100 * time.Millisecond, NoSync: !fsync})
	if err != nil {
		return nil, fmt.Errorf("open metadata: %w", err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(bucketsTable); err != nil {
			return err
		}
		_, err := tx.CreateBucketIfNotExists(objectsTable)
		return err
	}); err != nil {
		db.Close()
		return nil, fmt.Errorf("initialize metadata: %w", err)
	}
	db.MaxBatchSize = 256
	db.MaxBatchDelay = time.Millisecond
	for _, dir := range []string{"blobs", "tmp"} {
		if err := os.MkdirAll(filepath.Join(root, dir), 0o750); err != nil {
			db.Close()
			return nil, err
		}
	}
	if entries, err := os.ReadDir(filepath.Join(root, "tmp")); err == nil {
		for _, entry := range entries {
			_ = os.RemoveAll(filepath.Join(root, "tmp", entry.Name()))
		}
	}
	s := &Store{root: root, db: db, fsync: fsync, reclaim: make(chan string, 4096), closed: make(chan struct{})}
	s.wg.Add(1)
	go s.reclaimer()
	return s, nil
}

func (s *Store) Close() error {
	select {
	case <-s.closed:
		return nil
	default:
		close(s.closed)
	}
	s.wg.Wait()
	return s.db.Close()
}

func (s *Store) reclaimer() {
	defer s.wg.Done()
	for {
		select {
		case id := <-s.reclaim:
			_ = os.Remove(s.blobPath(id))
		case <-s.closed:
			for {
				select {
				case id := <-s.reclaim:
					_ = os.Remove(s.blobPath(id))
				default:
					return
				}
			}
		}
	}
}

func (s *Store) queueReclaim(id string) {
	if id == "" {
		return
	}
	select {
	case s.reclaim <- id:
	case <-s.closed:
	}
}

func (s *Store) ListBuckets(_ context.Context) ([]storage.Bucket, error) {
	var out []storage.Bucket
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketsTable).ForEach(func(_, value []byte) error {
			var bucket storage.Bucket
			if err := json.Unmarshal(value, &bucket); err != nil {
				return err
			}
			out = append(out, bucket)
			return nil
		})
	})
	return out, err
}

func (s *Store) CreateBucket(_ context.Context, name string, public bool) error {
	if !validBucket(name) {
		return fmt.Errorf("invalid bucket name")
	}
	return s.db.Batch(func(tx *bolt.Tx) error {
		table := tx.Bucket(bucketsTable)
		if table.Get([]byte(name)) != nil {
			return storage.ErrBucketExists
		}
		value, _ := json.Marshal(storage.Bucket{Name: name, Created: time.Now().UTC(), PublicRead: public})
		return table.Put([]byte(name), value)
	})
}

func (s *Store) GetBucket(_ context.Context, name string) (storage.Bucket, error) {
	var out storage.Bucket
	err := s.db.View(func(tx *bolt.Tx) error {
		value := tx.Bucket(bucketsTable).Get([]byte(name))
		if value == nil {
			return storage.ErrNoSuchBucket
		}
		return json.Unmarshal(value, &out)
	})
	return out, err
}

func (s *Store) DeleteBucket(_ context.Context, name string) error {
	return s.db.Batch(func(tx *bolt.Tx) error {
		if tx.Bucket(bucketsTable).Get([]byte(name)) == nil {
			return storage.ErrNoSuchBucket
		}
		prefix := objectPrefix(name)
		key, _ := tx.Bucket(objectsTable).Cursor().Seek(prefix)
		if key != nil && bytes.HasPrefix(key, prefix) {
			return storage.ErrBucketNotEmpty
		}
		return tx.Bucket(bucketsTable).Delete([]byte(name))
	})
}

func (s *Store) PutObject(ctx context.Context, bucket, key string, src io.Reader, opts storage.PutOptions) (storage.Object, error) {
	if key == "" || len([]byte(key)) > 1024 {
		return storage.Object{}, fmt.Errorf("invalid object key")
	}
	if _, err := s.GetBucket(ctx, bucket); err != nil {
		return storage.Object{}, err
	}
	id, err := randomID()
	if err != nil {
		return storage.Object{}, err
	}
	tmp := filepath.Join(s.root, "tmp", id)
	file, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return storage.Object{}, err
	}
	md5Hash := md5.New()
	crcHash := crc32.New(castagnoli)
	shaHash := sha256.New()
	limit := opts.MaxSize
	if limit <= 0 {
		limit = 5 << 30
	}
	limited := &io.LimitedReader{R: &contextReader{ctx: ctx, r: src}, N: limit + 1}
	written, copyErr := io.CopyBuffer(io.MultiWriter(file, md5Hash, crcHash, shaHash), limited, make([]byte, 256<<10))
	if copyErr == nil && written > limit {
		copyErr = storage.ErrEntityTooLarge
	}
	if copyErr == nil && len(opts.SHA256) > 0 && !bytes.Equal(opts.SHA256, shaHash.Sum(nil)) {
		copyErr = fmt.Errorf("payload SHA256 mismatch")
	}
	if copyErr == nil && s.fsync {
		copyErr = syncFile(file)
	}
	if closeErr := file.Close(); copyErr == nil {
		copyErr = closeErr
	}
	if copyErr != nil {
		_ = os.Remove(tmp)
		return storage.Object{}, copyErr
	}
	path := s.blobPath(id)
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		_ = os.Remove(tmp)
		return storage.Object{}, err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return storage.Object{}, err
	}
	if s.fsync {
		if err := syncDir(filepath.Dir(path)); err != nil {
			_ = os.Remove(path)
			return storage.Object{}, err
		}
	}
	obj := storage.Object{
		Bucket: bucket, Key: key, Blob: id, Size: written,
		ETag: hex.EncodeToString(md5Hash.Sum(nil)), CRC32C: crcHash.Sum32(),
		LastModified: time.Now().UTC(), Metadata: opts.Metadata,
	}
	var old string
	err = s.db.Batch(func(tx *bolt.Tx) error {
		if tx.Bucket(bucketsTable).Get([]byte(bucket)) == nil {
			return storage.ErrNoSuchBucket
		}
		table := tx.Bucket(objectsTable)
		compound := objectKey(bucket, key)
		if value := table.Get(compound); value != nil {
			var current storage.Object
			if err := json.Unmarshal(value, &current); err != nil {
				return err
			}
			if opts.IfNoneMatch == "*" || (opts.IfMatch != "" && !etagMatches(opts.IfMatch, current.ETag)) {
				return storage.ErrPreconditionFailed
			}
			old = current.Blob
		} else if opts.IfMatch != "" {
			return storage.ErrPreconditionFailed
		}
		value, err := json.Marshal(obj)
		if err != nil {
			return err
		}
		return table.Put(compound, value)
	})
	if err != nil {
		_ = os.Remove(path)
		return storage.Object{}, err
	}
	s.queueReclaim(old)
	return obj, nil
}

func (s *Store) HeadObject(_ context.Context, bucket, key string) (storage.Object, error) {
	var out storage.Object
	err := s.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket(bucketsTable).Get([]byte(bucket)) == nil {
			return storage.ErrNoSuchBucket
		}
		value := tx.Bucket(objectsTable).Get(objectKey(bucket, key))
		if value == nil {
			return storage.ErrNoSuchKey
		}
		if err := json.Unmarshal(value, &out); err != nil {
			return err
		}
		out.Bucket = bucket
		return nil
	})
	return out, err
}

func (s *Store) OpenObject(_ context.Context, bucket, key string) (storage.Object, storage.ReadSeekCloser, error) {
	var out storage.Object
	var file *os.File
	err := s.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket(bucketsTable).Get([]byte(bucket)) == nil {
			return storage.ErrNoSuchBucket
		}
		value := tx.Bucket(objectsTable).Get(objectKey(bucket, key))
		if value == nil {
			return storage.ErrNoSuchKey
		}
		if err := json.Unmarshal(value, &out); err != nil {
			return err
		}
		out.Bucket = bucket
		var err error
		file, err = os.Open(s.blobPath(out.Blob))
		return err
	})
	return out, file, err
}

func (s *Store) DeleteObject(_ context.Context, bucket, key string) error {
	var blob string
	err := s.db.Batch(func(tx *bolt.Tx) error {
		if tx.Bucket(bucketsTable).Get([]byte(bucket)) == nil {
			return storage.ErrNoSuchBucket
		}
		table := tx.Bucket(objectsTable)
		compound := objectKey(bucket, key)
		if value := table.Get(compound); value != nil {
			var current storage.Object
			if err := json.Unmarshal(value, &current); err != nil {
				return err
			}
			blob = current.Blob
		}
		return table.Delete(compound)
	})
	if err == nil {
		s.queueReclaim(blob)
	}
	return err
}

func (s *Store) ListObjects(_ context.Context, bucket string, opts storage.ListOptions) (storage.ListResult, error) {
	if opts.Limit < 0 || opts.Limit > 1000 {
		opts.Limit = 1000
	}
	var result storage.ListResult
	err := s.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket(bucketsTable).Get([]byte(bucket)) == nil {
			return storage.ErrNoSuchBucket
		}
		if opts.Limit == 0 {
			return nil
		}
		prefix := objectPrefix(bucket)
		seek := objectKey(bucket, opts.Prefix)
		if opts.Marker > opts.Prefix {
			seek = objectKey(bucket, opts.Marker)
		}
		cursor := tx.Bucket(objectsTable).Cursor()
		seenPrefixes := make(map[string]struct{})
		for rawKey, value := cursor.Seek(seek); rawKey != nil && bytes.HasPrefix(rawKey, prefix); rawKey, value = cursor.Next() {
			key := string(rawKey[len(prefix):])
			if key <= opts.Marker || !strings.HasPrefix(key, opts.Prefix) {
				continue
			}
			if opts.Delimiter != "" {
				rest := strings.TrimPrefix(key, opts.Prefix)
				if index := strings.Index(rest, opts.Delimiter); index >= 0 {
					common := opts.Prefix + rest[:index+len(opts.Delimiter)]
					if _, exists := seenPrefixes[common]; exists {
						continue
					}
					if len(result.Objects)+len(result.Prefixes) >= opts.Limit {
						result.Truncated = true
						break
					}
					seenPrefixes[common] = struct{}{}
					result.Prefixes = append(result.Prefixes, common)
					result.NextMarker = common
					continue
				}
			}
			if len(result.Objects)+len(result.Prefixes) >= opts.Limit {
				result.Truncated = true
				break
			}
			var obj storage.Object
			if err := json.Unmarshal(value, &obj); err != nil {
				return err
			}
			obj.Bucket = bucket
			result.Objects = append(result.Objects, obj)
			result.NextMarker = key
		}
		if !result.Truncated {
			result.NextMarker = ""
		}
		return nil
	})
	return result, err
}

func (s *Store) blobPath(id string) string {
	return filepath.Join(s.root, "blobs", id[:2], id[2:4], id)
}

func objectPrefix(bucket string) []byte   { return []byte(bucket + "\x00") }
func objectKey(bucket, key string) []byte { return []byte(bucket + "\x00" + key) }

func randomID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return syncFile(dir)
}

func validBucket(name string) bool {
	if len(name) < 3 || len(name) > 63 || name[0] == '-' || name[len(name)-1] == '-' || strings.Contains(name, "..") {
		return false
	}
	for _, char := range name {
		if (char < 'a' || char > 'z') && (char < '0' || char > '9') && char != '-' && char != '.' {
			return false
		}
	}
	return true
}

func etagMatches(header, etag string) bool {
	for _, candidate := range strings.Split(header, ",") {
		candidate = strings.Trim(strings.TrimSpace(strings.TrimPrefix(candidate, "W/")), "\"")
		if candidate == "*" || candidate == etag {
			return true
		}
	}
	return false
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r *contextReader) Read(p []byte) (int, error) {
	select {
	case <-r.ctx.Done():
		return 0, r.ctx.Err()
	default:
		return r.r.Read(p)
	}
}
