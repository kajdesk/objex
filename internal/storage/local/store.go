// Package local is the single-node storage engine: metadata in bbolt,
// object data in immutable blob files under <root>/blobs/ab/cd/<id>.
package local

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/kajdesk/objex/internal/s3err"
	"github.com/kajdesk/objex/internal/storage"
	bolt "go.etcd.io/bbolt"
)

// Options configures the engine.
type Options struct {
	// Fsync makes every acknowledged write durable.
	Fsync bool
	// VerifyReads checks the CRC32C of segments that are read in full.
	VerifyReads bool
}

// DefaultOptions are durable, verified reads.
func DefaultOptions() Options { return Options{Fsync: true, VerifyReads: true} }

// Store implements storage.Store.
type Store struct {
	root          string
	opts          Options
	db            *bolt.DB
	leases        *leases
	reclaim       *reclaimer
	durableDirs   sync.Map
	commits       chan *commitJob
	stopCommits   chan struct{}
	committerDone chan struct{}
	// maintenance serializes GC and scrub.
	maintenance sync.Mutex
	closeOnce   sync.Once
}

var _ storage.Store = (*Store)(nil)

// ErrInUse means another process has the data directory open.
var ErrInUse = errors.New("data directory is in use by another objex process")

// Open opens (creating if needed) the data directory at root. Only one process
// may use a data directory at a time.
func Open(root string, opts Options) (*Store, error) {
	if err := os.MkdirAll(root, 0o750); err != nil {
		return nil, fmt.Errorf("create data directory: %w", err)
	}
	// bbolt's file lock guards the whole directory: nothing below touches tmp/
	// or blobs/ until it is held.
	db, err := bolt.Open(filepath.Join(root, "meta.db"), 0o600, &bolt.Options{
		Timeout:        200 * time.Millisecond,
		NoSync:         !opts.Fsync,
		NoFreelistSync: true,
		FreelistType:   bolt.FreelistMapType,
	})
	if errors.Is(err, bolt.ErrTimeout) {
		return nil, fmt.Errorf("%s: %w", root, ErrInUse)
	}
	if err != nil {
		return nil, fmt.Errorf("open metadata: %w", err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		for _, t := range allTables {
			if _, err := tx.CreateBucketIfNotExists(t); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		db.Close()
		return nil, fmt.Errorf("initialize metadata: %w", err)
	}
	for _, dir := range []string{"blobs", "tmp"} {
		if err := os.MkdirAll(filepath.Join(root, dir), 0o750); err != nil {
			db.Close()
			return nil, err
		}
	}
	// Nothing can be in flight at startup, so temporary files are garbage.
	if entries, err := os.ReadDir(filepath.Join(root, "tmp")); err == nil {
		for _, e := range entries {
			_ = os.RemoveAll(filepath.Join(root, "tmp", e.Name()))
		}
	}
	if opts.Fsync {
		if err := syncDir(root); err != nil {
			db.Close()
			return nil, err
		}
	}
	s := &Store{root: root, opts: opts, db: db, leases: newLeases(),
		commits: make(chan *commitJob, maxCommitBatch), stopCommits: make(chan struct{}), committerDone: make(chan struct{})}
	go s.committer()
	s.reclaim = newReclaimer(s)
	return s, nil
}

// Close stops background work and closes the metadata database.
func (s *Store) Close() error {
	var err error
	s.closeOnce.Do(func() {
		s.reclaim.close()
		close(s.stopCommits)
		<-s.committerDone
		err = s.db.Close()
	})
	return err
}

// FlushReclaim deletes queued, unleased blobs now (tests, shutdown tooling).
func (s *Store) FlushReclaim() { s.reclaim.Flush() }

// ValidBucketName applies S3's strict DNS-compatible bucket naming rules.
func ValidBucketName(name string) bool {
	if len(name) < 3 || len(name) > 63 {
		return false
	}
	alnum := func(c byte) bool { return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') }
	for i := 0; i < len(name); i++ {
		if c := name[i]; !alnum(c) && c != '-' && c != '.' {
			return false
		}
	}
	if !alnum(name[0]) || !alnum(name[len(name)-1]) {
		return false
	}
	if strings.Contains(name, "..") || strings.Contains(name, ".-") || strings.Contains(name, "-.") {
		return false
	}
	return net.ParseIP(name) == nil // must not look like an IP address
}

func validateKey(key string) error {
	if key == "" {
		return s3err.InvalidArgument.WithMessage("Object key must not be empty")
	}
	if len(key) > storage.MaxKeyLength {
		return s3err.KeyTooLong
	}
	return nil
}

func (s *Store) ListBuckets(context.Context) ([]storage.Bucket, error) {
	var out []storage.Bucket
	err := s.view(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketsTable).ForEach(func(_, v []byte) error {
			b, err := dec[storage.Bucket](v)
			out = append(out, b)
			return err
		})
	})
	return out, err
}

func (s *Store) CreateBucket(_ context.Context, name string, public bool) error {
	if !ValidBucketName(name) {
		return s3err.InvalidBucketName
	}
	value := enc(storage.Bucket{Name: name, Created: time.Now().UTC().Truncate(time.Millisecond), PublicRead: public})
	return s.update(func(tx *bolt.Tx) error {
		t := tx.Bucket(bucketsTable)
		if t.Get([]byte(name)) != nil {
			return s3err.BucketAlreadyOwnedByYou
		}
		return t.Put([]byte(name), value)
	})
}

func (s *Store) GetBucket(_ context.Context, name string) (storage.Bucket, error) {
	var out storage.Bucket
	err := s.view(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketsTable).Get([]byte(name))
		if v == nil {
			return s3err.NoSuchBucket
		}
		var err error
		out, err = dec[storage.Bucket](v)
		return err
	})
	return out, err
}

func (s *Store) UpdateBucket(_ context.Context, name string, update func(*storage.Bucket)) error {
	return s.update(func(tx *bolt.Tx) error {
		t := tx.Bucket(bucketsTable)
		v := t.Get([]byte(name))
		if v == nil {
			return s3err.NoSuchBucket
		}
		b, err := dec[storage.Bucket](v)
		if err != nil {
			return err
		}
		update(&b)
		return t.Put([]byte(name), enc(b))
	})
}

func (s *Store) DeleteBucket(_ context.Context, name string) error {
	var garbage []segment
	err := s.update(func(tx *bolt.Tx) error {
		garbage = nil
		if err := requireBucket(tx, name); err != nil {
			return err
		}
		prefix := objectPrefix(name)
		if k, _ := tx.Bucket(objectsTable).Cursor().Seek(prefix); k != nil && strings.HasPrefix(string(k), string(prefix)) {
			return s3err.BucketNotEmpty
		}
		// In-progress multipart uploads are aborted with the bucket.
		uploads := tx.Bucket(uploadsTable)
		var keys [][]byte
		c := uploads.Cursor()
		for k, _ := c.Seek(prefix); k != nil && strings.HasPrefix(string(k), string(prefix)); k, _ = c.Next() {
			keys = append(keys, append([]byte(nil), k...))
		}
		for _, k := range keys {
			id := string(k[strings.LastIndexByte(string(k), 0)+1:])
			blobs, err := removeParts(tx, id)
			if err != nil {
				return err
			}
			if err := dropRefs(tx, blobs); err != nil {
				return err
			}
			garbage = append(garbage, blobs...)
			if err := uploads.Delete(k); err != nil {
				return err
			}
		}
		return tx.Bucket(bucketsTable).Delete([]byte(name))
	})
	if err == nil {
		s.reclaim.queue(garbage)
	}
	return err
}
