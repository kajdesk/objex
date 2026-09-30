package local

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/kajdesk/objex/internal/checksum"
	"github.com/kajdesk/objex/internal/s3err"
	"github.com/kajdesk/objex/internal/storage"
)

const writeBuffer = 1 << 20

var castagnoli = checksum.Castagnoli()

var writeBufs = sync.Pool{New: func() any { b := make([]byte, writeBuffer); return &b }}

func randomID() string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		panic(err) // crypto/rand does not fail on supported platforms
	}
	return hex.EncodeToString(raw[:])
}

func (s *Store) blobPath(id string) string {
	return filepath.Join(s.root, "blobs", id[:2], id[2:4], id)
}

// digest is what writing a blob computed.
type digest struct {
	seg      segment
	md5      []byte
	checksum *checksum.Checksum
}

// hasher computes the ETag, internal CRC32C and any requested checksums, and
// verifies client-supplied digests.
type hasher struct {
	md5    hash.Hash
	crc    uint32
	sha    hash.Hash
	extra  *checksum.Hasher
	expect storage.Expect
	size   int64
}

func newHasher(e storage.Expect) *hasher {
	h := &hasher{md5: md5.New(), expect: e}
	if len(e.SHA256) > 0 {
		h.sha = sha256.New()
	}
	algo := e.Algo
	if e.Checksum != nil {
		algo = e.Checksum.Algo
	}
	// CRC32C requests reuse the internal CRC instead of hashing twice.
	if algo != "" && algo != checksum.CRC32C {
		h.extra = checksum.New(algo)
	}
	return h
}

func (h *hasher) write(p []byte) {
	h.md5.Write(p)
	h.crc = crc32.Update(h.crc, castagnoli, p)
	if h.sha != nil {
		h.sha.Write(p)
	}
	if h.extra != nil {
		h.extra.Write(p)
	}
	h.size += int64(len(p))
}

// finish verifies every expectation and returns the results.
func (h *hasher) finish() (digest, error) {
	e := h.expect
	if e.Size >= 0 && e.Size != h.size {
		return digest{}, s3err.IncompleteBody
	}
	sum := h.md5.Sum(nil)
	if len(e.ContentMD5) > 0 && !bytes.Equal(e.ContentMD5, sum) {
		return digest{}, s3err.BadDigest
	}
	if h.sha != nil && !bytes.Equal(e.SHA256, h.sha.Sum(nil)) {
		return digest{}, s3err.XAmzContentSHA256Mismatch
	}
	d := digest{seg: segment{Size: h.size, CRC32C: h.crc}, md5: sum}
	algo := e.Algo
	if e.Checksum != nil {
		algo = e.Checksum.Algo
	}
	if algo != "" {
		var got checksum.Checksum
		if h.extra != nil {
			got = h.extra.Sum()
		} else {
			got = checksum.FromCRC32C(h.crc)
		}
		want := e.Checksum
		if e.Trailer != nil {
			if t, ok := e.Trailer(); ok {
				want = &t
			}
		}
		if want != nil {
			if want.Algo != got.Algo {
				return digest{}, s3err.InvalidRequest.WithMessage("Checksum algorithm mismatch")
			}
			if want.Value != got.Value {
				return digest{}, s3err.BadDigest.WithMessagef("The %s you specified did not match the calculated checksum.", got.Algo)
			}
		}
		d.checksum = &got
	}
	return d, nil
}

// writeBlob streams body into a new blob, hashing as it goes. The blob is
// durable and verified when this returns; on failure nothing is left behind.
// Reading/hashing and writing to disk overlap: a writer goroutine drains
// filled buffers while the next one is read.
func (s *Store) writeBlob(ctx context.Context, body io.Reader, e storage.Expect, maxSize int64) (digest, error) {
	id := randomID()
	tmp := filepath.Join(s.root, "tmp", id)
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return digest{}, s3err.Internal(err)
	}
	h := newHasher(e)

	type chunk struct {
		buf *[]byte
		n   int
	}
	full := make(chan chunk, 2)
	writeErr := make(chan error, 1)
	go func() {
		var werr error
		for c := range full {
			if werr == nil {
				_, werr = f.Write((*c.buf)[:c.n])
			}
			writeBufs.Put(c.buf)
		}
		writeErr <- werr
	}()

	var rerr error
	for rerr == nil {
		if err := ctx.Err(); err != nil {
			rerr = err
			break
		}
		buf := writeBufs.Get().(*[]byte)
		n, err := io.ReadFull(body, *buf)
		if n > 0 {
			if h.size+int64(n) > maxSize {
				writeBufs.Put(buf)
				rerr = s3err.EntityTooLarge
				break
			}
			h.write((*buf)[:n])
			full <- chunk{buf, n}
		} else {
			writeBufs.Put(buf)
		}
		switch {
		case err == nil:
		case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
			rerr = io.EOF
		default:
			rerr = err
		}
	}
	close(full)
	werr := <-writeErr

	var d digest
	switch {
	case !errors.Is(rerr, io.EOF):
		err = bodyError(rerr)
	case werr != nil:
		err = s3err.Internal(werr)
	default:
		d, err = h.finish()
	}
	if err == nil && s.opts.Fsync {
		err = s3err.Internal(syncFile(f))
	}
	if cerr := f.Close(); err == nil {
		err = s3err.Internal(cerr)
	}
	if err == nil {
		err = s.place(tmp, id, false)
	}
	if err != nil {
		_ = os.Remove(tmp)
		return digest{}, err
	}
	d.seg.Blob = id
	return d, nil
}

// bodyError maps a failure reading the request body. S3 errors (from
// aws-chunked decoding) pass through; anything else means the body was cut off.
func bodyError(err error) error {
	var e *s3err.Error
	if errors.As(err, &e) {
		return e
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return s3err.IncompleteBody.WithMessage("The request was cancelled before the body was received")
	}
	return s3err.IncompleteBody.WithMessage("Error reading the request body: " + err.Error())
}

// place moves (or hard-links) from into position as blob id. With fsync, the
// shard directories, and the new entry itself, are durable on return.
func (s *Store) place(from, id string, link bool) error {
	dst := s.blobPath(id)
	leaf := filepath.Dir(dst)
	mid := filepath.Dir(leaf)
	for _, d := range [][2]string{{mid, filepath.Join(s.root, "blobs")}, {leaf, mid}} {
		if err := s.ensureDir(d[0], d[1]); err != nil {
			return err
		}
	}
	if link {
		if err := os.Link(from, dst); err != nil {
			if err := copyFile(from, dst, s.opts.Fsync); err != nil {
				return s3err.Internal(err)
			}
		}
	} else if err := os.Rename(from, dst); err != nil {
		return s3err.Internal(err)
	}
	if s.opts.Fsync {
		return s3err.Internal(syncDir(leaf))
	}
	return nil
}

// ensureDir creates dir if needed and, with fsync, makes its entry in parent
// durable once. The parent is synced even when another writer created dir, as
// that writer may not have synced it yet.
func (s *Store) ensureDir(dir, parent string) error {
	if _, ok := s.durableDirs.Load(dir); ok {
		return nil
	}
	if err := os.Mkdir(dir, 0o750); err != nil && !errors.Is(err, os.ErrExist) {
		return s3err.Internal(err)
	}
	if s.opts.Fsync {
		if err := syncDir(parent); err != nil {
			return s3err.Internal(err)
		}
	}
	s.durableDirs.Store(dir, struct{}{})
	return nil
}

// duplicate makes an independent copy of a blob (a hard link when possible).
func (s *Store) duplicate(src segment) (segment, error) {
	id := randomID()
	if err := s.place(s.blobPath(src.Blob), id, true); err != nil {
		return segment{}, err
	}
	return segment{Blob: id, Size: src.Size, CRC32C: src.CRC32C}, nil
}

func (s *Store) removeBlob(id string) error {
	err := os.Remove(s.blobPath(id))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func copyFile(from, to string, fsync bool) error {
	in, err := os.Open(from)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(to, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, err = io.Copy(out, in)
	if err == nil && fsync {
		err = syncFile(out)
	}
	if cerr := out.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		_ = os.Remove(to)
	}
	return err
}

func syncDir(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	defer d.Close()
	return syncFile(d)
}
