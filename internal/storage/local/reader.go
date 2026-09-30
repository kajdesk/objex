package local

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"sort"
	"sync"

	"github.com/kajdesk/objex/internal/s3err"
)

const readChunk = 256 << 10

var readBufs = sync.Pool{New: func() any { b := make([]byte, readChunk); return &b }}

// ErrIntegrity reports a missing, truncated or corrupt blob.
var ErrIntegrity = errors.New("integrity error")

func integrityError(id, what string) error {
	slog.Error("integrity: blob damaged", "blob", id, "problem", what)
	return fmt.Errorf("%w: blob %s: %s", ErrIntegrity, id, what)
}

// objectReader streams an object's segments, opening each only when the
// requested range reaches it. Its leases keep the blobs alive until Close.
type objectReader struct {
	s      *Store
	segs   []segment
	starts []int64
	ids    []string
	once   sync.Once
}

func newObjectReader(s *Store, segs []segment) *objectReader {
	r := &objectReader{s: s, segs: segs, starts: make([]int64, len(segs)), ids: make([]string, len(segs))}
	var at int64
	for i, sg := range segs {
		r.starts[i], r.ids[i] = at, sg.Blob
		at += sg.Size
	}
	return r
}

func (r *objectReader) Close() error {
	r.once.Do(func() { r.s.leases.release(r.ids) })
	return nil
}

func (r *objectReader) PartRange(n int) (int64, int64, bool) {
	if n < 1 || n > len(r.segs) {
		return 0, 0, false
	}
	return r.starts[n-1], r.segs[n-1].Size, true
}

// open opens segment i and checks its size, so a missing or truncated blob
// fails before any of it is sent.
func (r *objectReader) open(i int) (*os.File, error) {
	sg := r.segs[i]
	f, err := os.Open(r.s.blobPath(sg.Blob))
	if errors.Is(err, os.ErrNotExist) {
		return nil, integrityError(sg.Blob, "blob file is missing")
	}
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err == nil && st.Size() != sg.Size {
		err = integrityError(sg.Blob, fmt.Sprintf("size is %d, expected %d", st.Size(), sg.Size))
	}
	if err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

// Check opens the segment holding byte start, so the caller can fail the
// request before sending headers when data is missing.
func (r *objectReader) Check(start int64) error {
	if len(r.segs) == 0 {
		return nil
	}
	f, err := r.open(r.locate(start))
	if err != nil {
		return s3err.Internal(err)
	}
	return f.Close()
}

func (r *objectReader) locate(pos int64) int {
	i := sort.Search(len(r.starts), func(i int) bool { return r.starts[i] > pos }) - 1
	if i < 0 {
		i = 0
	}
	return i
}

func (r *objectReader) CopyRange(w io.Writer, start, length int64) error {
	for i := r.locate(start); length > 0 && i < len(r.segs); i++ {
		sg := r.segs[i]
		off := start - r.starts[i]
		n := min(sg.Size-off, length)
		f, err := r.open(i)
		if err != nil {
			return err
		}
		if off == 0 && n == sg.Size && r.s.opts.VerifyReads && sg.CRC32C != 0 {
			err = copyVerified(w, f, sg)
		} else {
			err = copySection(w, f, off, n, sg.Blob)
		}
		f.Close()
		if err != nil {
			return err
		}
		start += n
		length -= n
	}
	return nil
}

// copySection sends a byte range unverified. Going through *os.File and
// io.LimitedReader lets net/http use sendfile.
func copySection(w io.Writer, f *os.File, off, n int64, id string) error {
	if _, err := f.Seek(off, io.SeekStart); err != nil {
		return err
	}
	copied, err := io.Copy(w, &io.LimitedReader{R: f, N: n})
	if err == nil && copied < n {
		err = integrityError(id, "blob is truncated")
	}
	return err
}

// copyVerified sends a whole segment, checking its CRC32C. The final chunk is
// held back until the checksum verifies, so a client never receives corrupt
// data as a complete response.
func copyVerified(w io.Writer, f *os.File, sg segment) error {
	bp := readBufs.Get().(*[]byte)
	defer readBufs.Put(bp)
	buf := *bp
	var crc uint32
	left := sg.Size
	for left > 0 {
		n := int(min(int64(len(buf)), left))
		if _, err := io.ReadFull(f, buf[:n]); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return integrityError(sg.Blob, "blob is truncated")
			}
			return err
		}
		crc = crc32.Update(crc, castagnoli, buf[:n])
		left -= int64(n)
		if left == 0 && crc != sg.CRC32C {
			return integrityError(sg.Blob, "checksum mismatch")
		}
		if _, err := w.Write(buf[:n]); err != nil {
			return err
		}
	}
	return nil
}
