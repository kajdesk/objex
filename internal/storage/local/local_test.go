package local

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kajdesk/objex/internal/checksum"
	"github.com/kajdesk/objex/internal/s3err"
	"github.com/kajdesk/objex/internal/storage"
)

var ctx = context.Background()

func open(t *testing.T) (*Store, string) {
	t.Helper()
	dir := t.TempDir()
	s, err := Open(dir, Options{VerifyReads: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, dir
}

func mustBucket(t *testing.T, s *Store, name string) {
	t.Helper()
	if err := s.CreateBucket(ctx, name, false); err != nil {
		t.Fatal(err)
	}
}

func put(t *testing.T, s *Store, b, k string, data []byte) storage.Object {
	t.Helper()
	o, err := s.PutObject(ctx, b, k, bytes.NewReader(data), storage.PutOptions{Expect: storage.Expect{Size: -1}})
	if err != nil {
		t.Fatalf("put %s/%s: %v", b, k, err)
	}
	return o
}

func read(t *testing.T, s *Store, b, k string) []byte {
	t.Helper()
	o, r, err := s.OpenObject(ctx, b, k)
	if err != nil {
		t.Fatalf("open %s/%s: %v", b, k, err)
	}
	defer r.Close()
	var buf bytes.Buffer
	if err := r.CopyRange(&buf, 0, o.Size); err != nil {
		t.Fatalf("read %s/%s: %v", b, k, err)
	}
	return buf.Bytes()
}

func blobs(t *testing.T, s *Store, dir string) []string {
	t.Helper()
	s.FlushReclaim()
	var out []string
	_ = filepath.Walk(filepath.Join(dir, "blobs"), func(p string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			out = append(out, p)
		}
		return nil
	})
	return out
}

func data(n int, seed byte) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*7) + seed
	}
	return b
}

func wantCode(t *testing.T, err error, code *s3err.Error) {
	t.Helper()
	if !errors.Is(err, code) {
		t.Fatalf("want %s, got %v", code.Code, err)
	}
}

func TestPutGetOverwriteDelete(t *testing.T) {
	s, dir := open(t)
	mustBucket(t, s, "bk1")
	wantCode(t, s.CreateBucket(ctx, "bk1", false), s3err.BucketAlreadyOwnedByYou)
	wantCode(t, s.CreateBucket(ctx, "Bad_Name", false), s3err.InvalidBucketName)
	o := put(t, s, "bk1", "a/b.txt", []byte("hello"))
	if o.ETag != "5d41402abc4b2a76b9719d911017c592" {
		t.Fatal(o.ETag)
	}
	put(t, s, "bk1", "a/b.txt", []byte("world!"))
	if got := read(t, s, "bk1", "a/b.txt"); string(got) != "world!" {
		t.Fatal(string(got))
	}
	if n := len(blobs(t, s, dir)); n != 1 {
		t.Fatalf("overwritten blob not reclaimed: %d blobs", n)
	}
	wantCode(t, s.DeleteBucket(ctx, "bk1"), s3err.BucketNotEmpty)
	if _, err := s.DeleteObjects(ctx, "bk1", []string{"a/b.txt", "missing"}); err != nil {
		t.Fatal(err)
	}
	_, err := s.HeadObject(ctx, "bk1", "a/b.txt")
	wantCode(t, err, s3err.NoSuchKey)
	if n := len(blobs(t, s, dir)); n != 0 {
		t.Fatalf("%d blobs left", n)
	}
	if err := s.DeleteBucket(ctx, "bk1"); err != nil {
		t.Fatal(err)
	}
	_, err = s.HeadObject(ctx, "bk1", "x")
	wantCode(t, err, s3err.NoSuchBucket)
}

func TestConditionalPut(t *testing.T) {
	s, _ := open(t)
	mustBucket(t, s, "bk1")
	opts := func(c storage.WriteConditions) storage.PutOptions {
		return storage.PutOptions{Cond: c, Expect: storage.Expect{Size: -1}}
	}
	o, err := s.PutObject(ctx, "bk1", "k", strings.NewReader("1"), opts(storage.WriteConditions{IfNoneMatch: "*"}))
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.PutObject(ctx, "bk1", "k", strings.NewReader("2"), opts(storage.WriteConditions{IfNoneMatch: "*"}))
	wantCode(t, err, s3err.PreconditionFailed)
	_, err = s.PutObject(ctx, "bk1", "k", strings.NewReader("2"), opts(storage.WriteConditions{IfNoneMatch: `"` + o.ETag + `"`}))
	wantCode(t, err, s3err.PreconditionFailed)
	_, err = s.PutObject(ctx, "bk1", "k", strings.NewReader("2"), opts(storage.WriteConditions{IfMatch: `"nope"`}))
	wantCode(t, err, s3err.PreconditionFailed)
	if _, err := s.PutObject(ctx, "bk1", "k", strings.NewReader("3"), opts(storage.WriteConditions{IfMatch: o.ETag})); err != nil {
		t.Fatal(err)
	}
	if got := read(t, s, "bk1", "k"); string(got) != "3" {
		t.Fatal(string(got))
	}
}

func TestExpectations(t *testing.T) {
	s, _ := open(t)
	mustBucket(t, s, "bk1")
	try := func(e storage.Expect) error {
		_, err := s.PutObject(ctx, "bk1", "k", strings.NewReader("hello"), storage.PutOptions{Expect: e})
		return err
	}
	wantCode(t, try(storage.Expect{Size: 6}), s3err.IncompleteBody)
	wantCode(t, try(storage.Expect{Size: -1, ContentMD5: make([]byte, 16)}), s3err.BadDigest)
	wantCode(t, try(storage.Expect{Size: -1, SHA256: make([]byte, 32)}), s3err.XAmzContentSHA256Mismatch)
	bad := checksum.Checksum{Algo: checksum.CRC32, Value: "AAAAAA=="}
	wantCode(t, try(storage.Expect{Size: -1, Checksum: &bad}), s3err.BadDigest)
	h := checksum.New(checksum.SHA256)
	_, _ = h.Write([]byte("hello"))
	good := h.Sum()
	o, err := s.PutObject(ctx, "bk1", "k", strings.NewReader("hello"), storage.PutOptions{Expect: storage.Expect{Size: 5, Checksum: &good}})
	if err != nil || o.Checksum == nil || *o.Checksum != good {
		t.Fatalf("%v %v", o.Checksum, err)
	}
	// CRC32C reuses the internal CRC.
	o, err = s.PutObject(ctx, "bk1", "c", strings.NewReader("123456789"), storage.PutOptions{Expect: storage.Expect{Size: -1, Algo: checksum.CRC32C}})
	if err != nil || o.Checksum.Value != checksum.FromCRC32C(0xE3069283).Value {
		t.Fatalf("%v %v", o.Checksum, err)
	}
}

func TestListing(t *testing.T) {
	s, _ := open(t)
	for _, b := range []string{"bk0", "bk1", "bk2"} {
		mustBucket(t, s, b)
	}
	put(t, s, "bk0", "zzz", nil)
	put(t, s, "bk2", "aaa", nil)
	for _, k := range []string{"a", "b/1", "b/2", "b/3/x", "c/1", "d"} {
		put(t, s, "bk1", k, []byte("x"))
	}
	list := func(prefix, delim, marker string, limit int) storage.ListResult {
		r, err := s.ListObjects(ctx, "bk1", storage.ListOptions{Prefix: prefix, Delimiter: delim, Marker: marker, Limit: limit})
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	keys := func(r storage.ListResult) []string {
		var out []string
		for _, o := range r.Objects {
			out = append(out, o.Key)
		}
		return out
	}
	eq := func(got, want []string) {
		t.Helper()
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("got %v want %v", got, want)
		}
	}
	eq(keys(list("", "", "", 1000)), []string{"a", "b/1", "b/2", "b/3/x", "c/1", "d"})
	r := list("", "/", "", 1000)
	eq(keys(r), []string{"a", "d"})
	eq(r.Prefixes, []string{"b/", "c/"})
	r = list("b/", "/", "", 1000)
	eq(keys(r), []string{"b/1", "b/2"})
	eq(r.Prefixes, []string{"b/3/"})

	// Page one entry at a time through a delimiter listing. The Go port used to
	// return a common prefix again on the page after it, forever.
	var seen []string
	marker := ""
	for i := 0; ; i++ {
		if i > 20 {
			t.Fatal("pagination does not terminate")
		}
		r := list("", "/", marker, 1)
		seen = append(append(seen, keys(r)...), r.Prefixes...)
		if !r.Truncated {
			break
		}
		marker = r.NextMarker
	}
	eq(seen, []string{"a", "b/", "c/", "d"})
	r = list("", "", "b/2", 2)
	eq(keys(r), []string{"b/3/x", "c/1"})
	if !r.Truncated || r.NextMarker != "c/1" {
		t.Fatalf("%+v", r)
	}
	if r := list("", "", "", 0); len(r.Objects) != 0 || r.Truncated {
		t.Fatalf("%+v", r)
	}
}

func TestListingSkipsPrefixesInOneSeek(t *testing.T) {
	s, _ := open(t)
	mustBucket(t, s, "bk1")
	for i := 0; i < 3000; i++ {
		put(t, s, "bk1", fmt.Sprintf("deep/%05d", i), nil)
	}
	put(t, s, "bk1", "top", nil)
	start := time.Now()
	r, err := s.ListObjects(ctx, "bk1", storage.ListOptions{Delimiter: "/", Limit: 1000})
	if err != nil || len(r.Prefixes) != 1 || len(r.Objects) != 1 {
		t.Fatalf("%+v %v", r, err)
	}
	if d := time.Since(start); d > 50*time.Millisecond {
		t.Fatalf("listing took %v; common prefixes should be skipped, not scanned", d)
	}
}

func TestMultipart(t *testing.T) {
	s, dir := open(t)
	mustBucket(t, s, "bk1")
	id, err := s.CreateMultipart(ctx, "bk1", "big", storage.Metadata{ContentType: "video/mp4"}, checksum.CRC32, checksum.FullObject)
	if err != nil {
		t.Fatal(err)
	}
	p1, p2 := data(storage.MinPartSize, 1), data(1000, 2)
	var parts []storage.Part
	for i, d := range [][]byte{p1, p2, p2} {
		p, err := s.UploadPart(ctx, "bk1", "big", id, i+1, bytes.NewReader(d), storage.Expect{Size: -1})
		if err != nil || p.Checksum == nil {
			t.Fatalf("part %d: %+v %v", i+1, p, err)
		}
		parts = append(parts, p)
	}
	if _, err := s.UploadPart(ctx, "bk1", "big", id, 1, bytes.NewReader(p1), storage.Expect{Size: -1}); err != nil {
		t.Fatal(err) // replacing a part
	}
	lp, err := s.ListParts(ctx, "bk1", "big", id, 0, 1000)
	if err != nil || len(lp.Parts) != 3 {
		t.Fatalf("%+v %v", lp, err)
	}
	lp, _ = s.ListParts(ctx, "bk1", "big", id, 1, 1)
	if len(lp.Parts) != 1 || lp.Parts[0].Number != 2 || !lp.Truncated {
		t.Fatalf("part pagination: %+v", lp)
	}
	ups, err := s.ListUploads(ctx, "bk1", storage.ListUploadsOptions{Limit: 10})
	if err != nil || len(ups.Uploads) != 1 {
		t.Fatalf("%+v %v", ups, err)
	}
	complete := func(nums ...int) []storage.CompletePart {
		var out []storage.CompletePart
		for _, n := range nums {
			out = append(out, storage.CompletePart{Number: n, ETag: parts[n-1].ETag})
		}
		return out
	}
	_, err = s.CompleteMultipart(ctx, "bk1", "big", id, complete(2, 1), storage.CompleteOptions{})
	wantCode(t, err, s3err.InvalidPartOrder)
	_, err = s.CompleteMultipart(ctx, "bk1", "big", id, complete(2, 3), storage.CompleteOptions{})
	wantCode(t, err, s3err.EntityTooSmall)
	o, err := s.CompleteMultipart(ctx, "bk1", "big", id, complete(1, 2), storage.CompleteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(o.ETag, "-2") || len(o.Parts) != 2 || o.Metadata.ContentType != "video/mp4" {
		t.Fatalf("%+v", o)
	}
	all := append(append([]byte{}, p1...), p2...)
	if !bytes.Equal(read(t, s, "bk1", "big"), all) {
		t.Fatal("multipart data mismatch")
	}
	h := checksum.New(checksum.CRC32)
	_, _ = h.Write(all)
	if *o.Checksum != h.Sum() {
		t.Fatalf("full-object CRC %v, want %v", o.Checksum, h.Sum())
	}
	// Part 3 and the replaced part 1 are reclaimed.
	if n := len(blobs(t, s, dir)); n != 2 {
		t.Fatalf("%d blobs", n)
	}
	_, err = s.ListParts(ctx, "bk1", "big", id, 0, 10)
	wantCode(t, err, s3err.NoSuchUpload)

	// A ranged read across the part boundary.
	_, r, _ := s.OpenObject(ctx, "bk1", "big")
	var buf bytes.Buffer
	start := int64(len(p1) - 10)
	if err := r.CopyRange(&buf, start, 20); err != nil || !bytes.Equal(buf.Bytes(), all[start:start+20]) {
		t.Fatalf("ranged read: %v", err)
	}
	if st, n, ok := r.PartRange(2); !ok || st != int64(len(p1)) || n != 1000 {
		t.Fatalf("part range %d %d", st, n)
	}
	r.Close()
}

func TestCompositeChecksumIsStrict(t *testing.T) {
	s, _ := open(t)
	mustBucket(t, s, "bk1")
	for _, suffix := range []string{"-2", "-x", "", "-1"} {
		id, _ := s.CreateMultipart(ctx, "bk1", "k", storage.Metadata{}, checksum.CRC32, "")
		p, err := s.UploadPart(ctx, "bk1", "k", id, 1, strings.NewReader("data"), storage.Expect{Size: -1})
		if err != nil {
			t.Fatal(err)
		}
		c, _ := checksum.MakeComposite(checksum.CRC32, []checksum.Checksum{*p.Checksum})
		digest, _, _ := strings.Cut(c.Value, "-")
		want := checksum.Checksum{Algo: checksum.CRC32, Value: digest + suffix}
		_, err = s.CompleteMultipart(ctx, "bk1", "k", id, []storage.CompletePart{{Number: 1, ETag: p.ETag}}, storage.CompleteOptions{Checksum: &want})
		switch suffix {
		case "-1":
			if err != nil {
				t.Fatal(err)
			}
		case "-x":
			wantCode(t, err, s3err.InvalidRequest)
		default:
			wantCode(t, err, s3err.BadDigest)
		}
	}
}

// Parts uploaded concurrently with completion: the result is always
// consistent and readable (completion runs in one transaction).
func TestCompleteConcurrentWithUploadPart(t *testing.T) {
	s, _ := open(t)
	mustBucket(t, s, "bk1")
	for round := 0; round < 20; round++ {
		id, _ := s.CreateMultipart(ctx, "bk1", "k", storage.Metadata{}, "", "")
		p, _ := s.UploadPart(ctx, "bk1", "k", id, 1, strings.NewReader("version A"), storage.Expect{Size: -1})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = s.UploadPart(ctx, "bk1", "k", id, 1, strings.NewReader("version B"), storage.Expect{Size: -1})
		}()
		_, err := s.CompleteMultipart(ctx, "bk1", "k", id, []storage.CompletePart{{Number: 1, ETag: p.ETag}}, storage.CompleteOptions{})
		wg.Wait()
		if err != nil {
			wantCode(t, err, s3err.InvalidPart) // part replaced first: rejected cleanly
			_ = s.AbortMultipart(ctx, "bk1", "k", id)
			continue
		}
		s.FlushReclaim()
		if got := read(t, s, "bk1", "k"); string(got) != "version A" {
			t.Fatalf("round %d: %q", round, got)
		}
	}
}

func TestCopyAndPartCopy(t *testing.T) {
	s, dir := open(t)
	mustBucket(t, s, "bk1")
	put(t, s, "bk1", "src", []byte("copy me"))
	copyOpts := storage.CopyOptions{}
	if _, err := s.CopyObject(ctx, "bk1", "src", "bk1", "dst", copyOpts); err != nil {
		t.Fatal(err)
	}
	_, err := s.CopyObject(ctx, "bk1", "src", "bk1", "src", copyOpts)
	wantCode(t, err, s3err.InvalidRequest)
	_, err = s.CopyObject(ctx, "bk1", "src", "bk1", "x", storage.CopyOptions{SourceCond: storage.ReadConditions{IfMatch: `"no"`}})
	wantCode(t, err, s3err.PreconditionFailed)
	if _, err := s.DeleteObjects(ctx, "bk1", []string{"src"}); err != nil {
		t.Fatal(err)
	}
	if got := read(t, s, "bk1", "dst"); string(got) != "copy me" {
		t.Fatal(string(got))
	}
	id, _ := s.CreateMultipart(ctx, "bk1", "mp", storage.Metadata{}, "", "")
	p, err := s.UploadPartCopy(ctx, "bk1", "dst", &[2]int64{0, 3}, storage.ReadConditions{}, "bk1", "mp", id, 1)
	if err != nil || p.Size != 4 {
		t.Fatalf("%+v %v", p, err)
	}
	p, err = s.UploadPartCopy(ctx, "bk1", "dst", nil, storage.ReadConditions{}, "bk1", "mp", id, 2)
	if err != nil || p.Size != 7 {
		t.Fatalf("whole copy: %+v %v", p, err)
	}
	o, err := s.CompleteMultipart(ctx, "bk1", "mp", id, []storage.CompletePart{{Number: 2, ETag: p.ETag}}, storage.CompleteOptions{})
	if err != nil || o.Size != 7 {
		t.Fatal(err)
	}
	if got := read(t, s, "bk1", "mp"); string(got) != "copy me" {
		t.Fatal(string(got))
	}
	if n := len(blobs(t, s, dir)); n != 2 {
		t.Fatalf("%d blobs", n)
	}
}

func TestGC(t *testing.T) {
	s, dir := open(t)
	mustBucket(t, s, "bk1")
	put(t, s, "bk1", "keep", []byte("keep me"))
	orphan := filepath.Join(dir, "blobs", "00", "00", "0000aaaa")
	_ = os.MkdirAll(filepath.Dir(orphan), 0o750)
	_ = os.WriteFile(orphan, []byte("junk"), 0o600)
	if n, err := s.GC(time.Hour); err != nil || n != 0 {
		t.Fatalf("young orphan removed: %d %v", n, err)
	}
	if n, err := s.GC(0); err != nil || n != 1 {
		t.Fatalf("GC removed %d, %v", n, err)
	}
	if got := read(t, s, "bk1", "keep"); string(got) != "keep me" {
		t.Fatal("GC removed referenced data")
	}
}

func TestReadsSurviveOverwriteAndDelete(t *testing.T) {
	s, dir := open(t)
	mustBucket(t, s, "bk1")
	id, _ := s.CreateMultipart(ctx, "bk1", "k", storage.Metadata{}, "", "")
	a, _ := s.UploadPart(ctx, "bk1", "k", id, 1, bytes.NewReader(data(storage.MinPartSize, 1)), storage.Expect{Size: -1})
	b, _ := s.UploadPart(ctx, "bk1", "k", id, 2, strings.NewReader("tail"), storage.Expect{Size: -1})
	o, _ := s.CompleteMultipart(ctx, "bk1", "k", id, []storage.CompletePart{{Number: 1, ETag: a.ETag}, {Number: 2, ETag: b.ETag}}, storage.CompleteOptions{})
	_, r, err := s.OpenObject(ctx, "bk1", "k")
	if err != nil {
		t.Fatal(err)
	}
	// Overwrite, delete and GC while the reader holds its lease. Its segments
	// open lazily, so the second one must still exist when reached.
	put(t, s, "bk1", "k", []byte("new"))
	_, _ = s.DeleteObjects(ctx, "bk1", []string{"k"})
	s.FlushReclaim()
	if _, err := s.GC(0); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := r.CopyRange(&buf, 0, o.Size); err != nil || int64(buf.Len()) != o.Size {
		t.Fatalf("read after delete: %d bytes, %v", buf.Len(), err)
	}
	r.Close()
	if n := len(blobs(t, s, dir)); n != 0 {
		t.Fatalf("%d blobs left after the reader closed", n)
	}
}

func TestIntegrity(t *testing.T) {
	s, dir := open(t)
	mustBucket(t, s, "bk1")
	d := data(600_000, 4)
	put(t, s, "bk1", "k", d)
	put(t, s, "bk1", "fine", []byte("untouched"))
	if r, err := s.Scrub(0); err != nil || len(r.Problems) != 0 || r.Blobs != 2 {
		t.Fatalf("%+v %v", r, err)
	}
	var path string
	for _, p := range blobs(t, s, dir) {
		if st, _ := os.Stat(p); st.Size() == int64(len(d)) {
			path = p
		}
	}
	raw, _ := os.ReadFile(path)
	raw[500_000] ^= 0xff
	_ = os.WriteFile(path, raw, 0o600)

	o, r, _ := s.OpenObject(ctx, "bk1", "k")
	var buf bytes.Buffer
	err := r.CopyRange(&buf, 0, o.Size)
	r.Close()
	if !errors.Is(err, ErrIntegrity) || buf.Len() >= len(d) {
		t.Fatalf("corruption not caught: %d bytes, %v", buf.Len(), err)
	}
	// Ranged reads that don't cover the whole blob are served unverified.
	_, r, _ = s.OpenObject(ctx, "bk1", "k")
	buf.Reset()
	if err := r.CopyRange(&buf, 0, 10); err != nil {
		t.Fatal(err)
	}
	r.Close()
	rep, _ := s.Scrub(0)
	if len(rep.Problems) != 1 || !strings.Contains(rep.Problems[0], "bk1/k") || !strings.Contains(rep.Problems[0], "mismatch") {
		t.Fatalf("%v", rep.Problems)
	}
	_ = os.WriteFile(path, raw[:1000], 0o600)
	_, r, _ = s.OpenObject(ctx, "bk1", "k")
	if err := r.Check(0); err == nil {
		t.Fatal("truncation not caught at open")
	}
	r.Close()
	rep, _ = s.Scrub(0)
	if len(rep.Problems) != 1 || !strings.Contains(rep.Problems[0], "size is 1000") {
		t.Fatalf("%v", rep.Problems)
	}
}

func TestDataDirIsExclusive(t *testing.T) {
	_, dir := open(t)
	inflight := filepath.Join(dir, "tmp", "upload-in-progress")
	_ = os.WriteFile(inflight, []byte("partial"), 0o600)
	if _, err := Open(dir, Options{}); !errors.Is(err, ErrInUse) {
		t.Fatalf("second open: %v", err)
	}
	if got, _ := os.ReadFile(inflight); string(got) != "partial" {
		t.Fatal("second open touched tmp/")
	}
}

func TestConcurrentWritersDurable(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{Fsync: true, VerifyReads: true})
	if err != nil {
		t.Fatal(err)
	}
	mustBucket(t, s, "bk1")
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			put(t, s, "bk1", fmt.Sprintf("k%02d", i), []byte(fmt.Sprint(i)))
		}(i)
	}
	wg.Wait()
	_ = s.Close()
	s, err = Open(dir, Options{Fsync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	r, _ := s.ListObjects(ctx, "bk1", storage.ListOptions{Limit: 1000})
	var got []string
	for _, o := range r.Objects {
		got = append(got, o.Key)
	}
	if len(got) != 64 || !sort.StringsAreSorted(got) {
		t.Fatalf("after reopen: %v", got)
	}
}
