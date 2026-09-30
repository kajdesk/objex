package auth

import (
	"crypto/sha256"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	"github.com/kajdesk/objex/internal/checksum"
	"github.com/kajdesk/objex/internal/config"
	"github.com/kajdesk/objex/internal/s3err"
)

// Test vectors from the AWS S3 SigV4 documentation.
var docKey = config.Key{Name: "t", AccessKey: "AKIAIOSFODNN7EXAMPLE", SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"}

func docTime() time.Time { return time.Date(2013, 5, 24, 0, 0, 0, 0, time.UTC) }

func TestHeaderGetObject(t *testing.T) {
	req, _ := http.NewRequest("GET", "http://examplebucket.s3.amazonaws.com/test.txt", nil)
	req.Header.Set("Range", "bytes=0-9")
	req.Header.Set("X-Amz-Content-Sha256", EmptySHA256)
	req.Header.Set("X-Amz-Date", "20130524T000000Z")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;range;x-amz-content-sha256;x-amz-date,Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41")
	r, err := New([]config.Key{docKey}).Authenticate(req, docTime())
	if err != nil || r.Mode != SHA256 || r.Key == nil {
		t.Fatalf("got %+v, %v", r, err)
	}
	req.Header.Set("Range", "bytes=0-10")
	if _, err := New([]config.Key{docKey}).Authenticate(req, docTime()); !errors.Is(err, s3err.SignatureDoesNotMatch) {
		t.Fatalf("tampered header: %v", err)
	}
}

func TestPresigned(t *testing.T) {
	req, _ := http.NewRequest("GET", "http://examplebucket.s3.amazonaws.com/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404", nil)
	v := New([]config.Key{docKey})
	if _, err := v.Authenticate(req, docTime()); err != nil {
		t.Fatal(err)
	}
	if _, err := v.Authenticate(req, docTime().Add(86401*time.Second)); !errors.Is(err, s3err.AccessDenied) {
		t.Fatalf("expired: %v", err)
	}
}

func TestSkewAndKeys(t *testing.T) {
	req, _ := http.NewRequest("GET", "http://examplebucket.s3.amazonaws.com/test.txt", nil)
	req.Header.Set("X-Amz-Date", "20130524T000000Z")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;x-amz-date,Signature=00")
	v := New(nil)
	if _, err := v.Authenticate(req, docTime().Add(time.Hour)); !errors.Is(err, s3err.RequestTimeTooSkewed) {
		t.Fatalf("skew: %v", err)
	}
	if _, err := v.Authenticate(req, docTime()); !errors.Is(err, s3err.InvalidAccessKeyID) {
		t.Fatalf("unknown key: %v", err)
	}
	v.SetKeys([]config.Key{docKey}) // hot reload
	if _, err := v.Authenticate(req, docTime()); !errors.Is(err, s3err.SignatureDoesNotMatch) {
		t.Fatalf("after reload: %v", err)
	}
}

// "Signature Calculations for the Authorization Header: Transferring Payload in
// Multiple Chunks" example.
func docSigner() *ChunkSigner {
	return &ChunkSigner{
		key:     SigningKey(docKey.SecretKey, "20130524", "us-east-1", "s3"),
		amzDate: "20130524T000000Z", scope: "20130524/us-east-1/s3/aws4_request",
		prev: "4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9",
	}
}

func TestChunkSignatures(t *testing.T) {
	s := docSigner()
	c1 := sha256.Sum256([]byte(strings.Repeat("a", 65536)))
	c2 := sha256.Sum256([]byte(strings.Repeat("a", 1024)))
	c3 := sha256.Sum256(nil)
	for i, c := range []struct {
		h   []byte
		sig string
	}{
		{c1[:], "ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648"},
		{c2[:], "0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497"},
		{c3[:], "b6c6ea8a5354eaf15b3cb7646744f4275b71ea724fed81ceb9323e279d449df9"},
	} {
		if err := s.VerifyChunk(c.h, c.sig); err != nil {
			t.Fatalf("chunk %d: %v", i, err)
		}
	}
}

func TestSignedChunkedBody(t *testing.T) {
	body := "10000;chunk-signature=ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648\r\n" + strings.Repeat("a", 65536) +
		"\r\n400;chunk-signature=0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497\r\n" + strings.Repeat("a", 1024) +
		"\r\n0;chunk-signature=b6c6ea8a5354eaf15b3cb7646744f4275b71ea724fed81ceb9323e279d449df9\r\n\r\n"
	got, err := io.ReadAll(iotest.OneByteReader(NewChunkedReader(strings.NewReader(body), docSigner(), false)))
	if err != nil || len(got) != 65536+1024 {
		t.Fatalf("decoded %d bytes, %v", len(got), err)
	}
	tampered := strings.Replace(body, "aaaa", "aaab", 1)
	if _, err := io.ReadAll(NewChunkedReader(strings.NewReader(tampered), docSigner(), false)); !errors.Is(err, s3err.SignatureDoesNotMatch) {
		t.Fatalf("tampered: %v", err)
	}
}

func TestUnsignedTrailer(t *testing.T) {
	body := "5\r\nhello\r\n6\r\n world\r\n0\r\nx-amz-checksum-crc32:DUoRhQ==\r\n\r\n"
	r := NewChunkedReader(iotest.HalfReader(strings.NewReader(body)), nil, true)
	got, err := io.ReadAll(r)
	if err != nil || string(got) != "hello world" {
		t.Fatalf("%q %v", got, err)
	}
	c, ok := r.TrailingChecksum()
	if !ok || c != (checksum.Checksum{Algo: checksum.CRC32, Value: "DUoRhQ=="}) {
		t.Fatalf("trailer %v", c)
	}
	for _, bad := range []string{"5\r\nhelloXX0\r\n\r\n", "5\r\nhel", "zz\r\n"} {
		if _, err := io.ReadAll(NewChunkedReader(strings.NewReader(bad), nil, false)); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
}

func TestEncoding(t *testing.T) {
	if Encode("a b/c~*") != "a%20b%2Fc~%2A" || EncodePath("a b/c+") != "a%20b/c%2B" {
		t.Fatal(Encode("a b/c~*"), EncodePath("a b/c+"))
	}
	q := ParseQuery("prefix=a%2Bb&x=a+b&flag")
	if first(q, "prefix") != "a+b" || first(q, "x") != "a+b" || len(q) != 3 {
		t.Fatalf("%+v", q)
	}
}
