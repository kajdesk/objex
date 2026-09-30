package local

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"
)

// GC deletes blob files that no metadata references and no reader leases.
// Files whose inode changed within grace are skipped: they may belong to a
// write that has not committed yet. It returns the number removed.
func (s *Store) GC(grace time.Duration) (int, error) {
	s.maintenance.Lock()
	defer s.maintenance.Unlock()
	cutoff := time.Now().Add(-grace)
	root := filepath.Join(s.root, "blobs")
	removed := 0
	shards, err := os.ReadDir(root)
	if err != nil {
		return 0, err
	}
	for _, a := range shards {
		subs, err := os.ReadDir(filepath.Join(root, a.Name()))
		if err != nil {
			continue
		}
		for _, b := range subs {
			dir := filepath.Join(root, a.Name(), b.Name())
			entries, err := os.ReadDir(dir)
			if err != nil {
				continue
			}
			var candidates []string
			for _, e := range entries {
				info, err := e.Info()
				if err != nil || !changedBefore(info, cutoff) {
					continue
				}
				candidates = append(candidates, e.Name())
			}
			if len(candidates) == 0 {
				continue
			}
			var orphans []string
			if err := s.view(func(tx *bolt.Tx) error {
				refs := tx.Bucket(refsTable)
				for _, id := range candidates {
					if refs.Get([]byte(id)) == nil {
						orphans = append(orphans, id)
					}
				}
				return nil
			}); err != nil {
				return removed, err
			}
			free, _ := s.leases.unleased(orphans)
			for _, id := range free {
				if len(id) >= 4 && s.removeBlob(id) == nil {
					removed++
				}
			}
		}
	}
	return removed, nil
}

// ScrubReport is the outcome of Scrub.
type ScrubReport struct {
	Blobs, Bytes int64
	// Problems describes missing, truncated or corrupt blobs.
	Problems []string
}

type scrubItem struct {
	owner []byte // key in objects or parts
	table []byte
	seg   segment
}

const scrubPage = 256

// Scrub verifies every referenced blob against its size and CRC32C, reading at
// most rate bytes per second (0: unlimited). It works a page at a time, so it
// never holds a metadata transaction open for long.
func (s *Store) Scrub(rate int64) (ScrubReport, error) {
	s.maintenance.Lock()
	defer s.maintenance.Unlock()
	var report ScrubReport
	start := time.Now()
	for _, table := range [][]byte{objectsTable, partsTable} {
		var cursor []byte
		for {
			var page []scrubItem
			var last []byte
			err := s.view(func(tx *bolt.Tx) error {
				c := tx.Bucket(table).Cursor()
				k, v := c.First()
				if cursor != nil {
					k, v = c.Seek(cursor)
					if k != nil && bytes.Equal(k, cursor) {
						k, v = c.Next()
					}
				}
				for n := 0; k != nil && n < scrubPage; k, v = c.Next() {
					segs, err := segmentsOf(table, v)
					if err != nil {
						return err
					}
					owner := bytes.Clone(k)
					for _, sg := range segs {
						page = append(page, scrubItem{owner: owner, table: table, seg: sg})
					}
					last = owner
					n++
				}
				return nil
			})
			if err != nil {
				return report, err
			}
			for _, it := range page {
				report.Blobs++
				report.Bytes += it.seg.Size
				if problem := s.verifyBlob(it.seg); problem != "" && s.stillOwns(it) {
					msg := fmt.Sprintf("%s: blob %s: %s", describe(it), it.seg.Blob, problem)
					slog.Error("scrub: damaged blob", "detail", msg)
					report.Problems = append(report.Problems, msg)
				}
				if rate > 0 {
					// Pace to the average rate since the scrub started.
					if ahead := time.Duration(float64(report.Bytes)/float64(rate)*float64(time.Second)) - time.Since(start); ahead > 0 {
						time.Sleep(ahead)
					}
				}
			}
			if last == nil {
				break
			}
			cursor = last
		}
	}
	return report, nil
}

func segmentsOf(table, v []byte) ([]segment, error) {
	if bytes.Equal(table, objectsTable) {
		r, err := dec[objectRecord](v)
		return r.Segments, err
	}
	p, err := dec[partRecord](v)
	return []segment{p.Blob}, err
}

func describe(it scrubItem) string {
	if bytes.Equal(it.table, objectsTable) {
		i := bytes.IndexByte(it.owner, 0)
		return fmt.Sprintf("object %s/%s", it.owner[:i], it.owner[i+1:])
	}
	n := len(it.owner)
	return fmt.Sprintf("upload %s part %d", it.owner[:n-4], uint32(it.owner[n-4])<<24|uint32(it.owner[n-3])<<16|uint32(it.owner[n-2])<<8|uint32(it.owner[n-1]))
}

// verifyBlob reads a blob in full, returning a description of any problem.
func (s *Store) verifyBlob(sg segment) string {
	f, err := os.Open(s.blobPath(sg.Blob))
	if errors.Is(err, os.ErrNotExist) {
		return "blob file is missing"
	}
	if err != nil {
		return err.Error()
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return err.Error()
	}
	if st.Size() != sg.Size {
		return fmt.Sprintf("size is %d, expected %d", st.Size(), sg.Size)
	}
	if sg.CRC32C == 0 && sg.Size == 0 {
		return ""
	}
	h := crc32.New(castagnoli)
	bp := readBufs.Get().(*[]byte)
	defer readBufs.Put(bp)
	if _, err := io.CopyBuffer(h, f, *bp); err != nil {
		return err.Error()
	}
	if h.Sum32() != sg.CRC32C {
		return "checksum mismatch"
	}
	return ""
}

// stillOwns reports whether the metadata still references the blob, so a blob
// overwritten or deleted while being checked is not reported as damaged.
func (s *Store) stillOwns(it scrubItem) bool {
	owns := false
	_ = s.view(func(tx *bolt.Tx) error {
		v := tx.Bucket(it.table).Get(it.owner)
		if v == nil {
			return nil
		}
		segs, err := segmentsOf(it.table, v)
		if err != nil {
			return nil
		}
		for _, sg := range segs {
			if sg.Blob == it.seg.Blob {
				owns = true
			}
		}
		return nil
	})
	return owns
}
