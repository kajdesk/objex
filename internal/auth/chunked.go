package auth

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"errors"
	"hash"
	"io"
	"strconv"
	"strings"

	"github.com/kajdesk/objex/internal/checksum"
	"github.com/kajdesk/objex/internal/s3err"
)

const maxChunkLine = 4096

// ChunkedReader decodes a Content-Encoding: aws-chunked body, verifying chunk
// signatures when signed and collecting trailing headers when present.
// Read returns the decoded object bytes; io.EOF only after the final chunk
// (and trailer) verified, so a caller that stores the data until EOF never
// commits a tampered body.
type ChunkedReader struct {
	r       *bufio.Reader
	signer  *ChunkSigner
	trailer bool

	left     int64 // bytes left in the current chunk
	inChunk  bool
	sig      string
	hash     hash.Hash
	done     bool
	err      error
	trailers map[string]string
}

// NewChunkedReader wraps body. signer is nil for unsigned streaming.
func NewChunkedReader(body io.Reader, signer *ChunkSigner, trailer bool) *ChunkedReader {
	c := &ChunkedReader{r: bufio.NewReaderSize(body, 64<<10), signer: signer, trailer: trailer}
	if signer != nil {
		c.hash = sha256.New()
	}
	return c
}

func malformed(msg string) error {
	return s3err.IncompleteBody.WithMessage("Malformed aws-chunked body: " + msg)
}

func (c *ChunkedReader) Read(p []byte) (int, error) {
	if c.err != nil {
		return 0, c.err
	}
	for !c.inChunk {
		if c.done {
			return 0, io.EOF
		}
		if err := c.next(); err != nil {
			c.err = err
			return 0, err
		}
	}
	if int64(len(p)) > c.left {
		p = p[:c.left]
	}
	n, err := c.r.Read(p)
	c.left -= int64(n)
	if c.hash != nil {
		c.hash.Write(p[:n])
	}
	if c.left == 0 {
		if e := c.endChunk(); e != nil {
			c.err = e
			return n, e
		}
	} else if err != nil {
		if errors.Is(err, io.EOF) {
			err = malformed("unexpected end of chunk data")
		}
		c.err = err
		return n, err
	}
	return n, nil
}

// next reads a chunk header (or the final chunk and trailer).
func (c *ChunkedReader) next() error {
	line, err := c.line()
	if err != nil {
		return err
	}
	size, ext, _ := strings.Cut(line, ";")
	n, err := strconv.ParseInt(strings.TrimSpace(size), 16, 64)
	if err != nil || n < 0 {
		return malformed("invalid chunk size")
	}
	c.sig = ""
	if v, ok := strings.CutPrefix(strings.TrimSpace(ext), "chunk-signature="); ok {
		c.sig = strings.TrimSpace(v)
	}
	if c.signer != nil && c.sig == "" {
		return s3err.SignatureDoesNotMatch.WithMessage("Missing chunk signature")
	}
	if c.hash != nil {
		c.hash.Reset()
	}
	if n > 0 {
		c.left, c.inChunk = n, true
		return nil
	}
	// Final (empty) chunk.
	if err := c.verifyChunk(); err != nil {
		return err
	}
	if c.trailer {
		if err := c.readTrailers(); err != nil {
			return err
		}
	} else {
		// An optional final "\r\n".
		if _, err := c.line(); err != nil && !errors.Is(err, io.EOF) {
			return err
		}
	}
	c.done = true
	return nil
}

func (c *ChunkedReader) endChunk() error {
	c.inChunk = false
	var crlf [2]byte
	if _, err := io.ReadFull(c.r, crlf[:]); err != nil {
		return malformed("unexpected end of chunk")
	}
	if crlf != [2]byte{'\r', '\n'} {
		return malformed("chunk data longer than its declared size")
	}
	return c.verifyChunk()
}

func (c *ChunkedReader) verifyChunk() error {
	if c.signer == nil {
		return nil
	}
	return c.signer.VerifyChunk(c.hash.Sum(nil), c.sig)
}

// line reads one "\r\n"-terminated line. A final line without a terminator is
// tolerated. io.EOF means clean end of input.
func (c *ChunkedReader) line() (string, error) {
	var buf []byte
	for {
		frag, err := c.r.ReadSlice('\n')
		buf = append(buf, frag...)
		if len(buf) > maxChunkLine {
			return "", malformed("chunk header too long")
		}
		if err == nil {
			return string(bytes.TrimSuffix(bytes.TrimSuffix(buf, []byte("\n")), []byte("\r"))), nil
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if errors.Is(err, io.EOF) {
			if len(buf) == 0 {
				return "", io.EOF
			}
			return string(bytes.TrimSuffix(buf, []byte("\r"))), nil
		}
		return "", err
	}
}

func (c *ChunkedReader) readTrailers() error {
	c.trailers = map[string]string{}
	var names []string
	var signature string
	for {
		line, err := c.line()
		if errors.Is(err, io.EOF) || (err == nil && line == "") {
			break
		}
		if err != nil {
			return err
		}
		k, v, ok := strings.Cut(line, ":")
		if !ok {
			return malformed("invalid trailer")
		}
		k, v = strings.ToLower(strings.TrimSpace(k)), strings.TrimSpace(v)
		if k == "x-amz-trailer-signature" {
			signature = v
			continue
		}
		c.trailers[k] = v
		names = append(names, k)
	}
	if c.signer != nil {
		var canonical strings.Builder
		for _, k := range names {
			canonical.WriteString(k + ":" + c.trailers[k] + "\n")
		}
		if signature == "" {
			return s3err.SignatureDoesNotMatch.WithMessage("Missing trailer signature")
		}
		if err := c.signer.VerifyTrailer(canonical.String(), signature); err != nil {
			return err
		}
	}
	return nil
}

// TrailingChecksum returns a checksum sent as a trailer. Only meaningful after
// Read returned io.EOF.
func (c *ChunkedReader) TrailingChecksum() (checksum.Checksum, bool) {
	for k, v := range c.trailers {
		if a, ok := checksum.FromHeader(k); ok {
			return checksum.Checksum{Algo: a, Value: v}, true
		}
	}
	return checksum.Checksum{}, false
}
