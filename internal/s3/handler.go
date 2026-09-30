// Package s3 is the S3 HTTP API: request parsing, routing, authorization and
// responses.
package s3

import (
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/kajdesk/objex/internal/auth"
	"github.com/kajdesk/objex/internal/checksum"
	"github.com/kajdesk/objex/internal/s3err"
	"github.com/kajdesk/objex/internal/storage"
)

// HealthPath is an unauthenticated liveness endpoint. The underscore makes it
// an invalid bucket name, so it can never shadow a real bucket.
const HealthPath = "/_objex/health"

const (
	xmlns       = "http://s3.amazonaws.com/doc/2006-03-01/"
	maxXMLBody  = 4 << 20 // DeleteObjects with 1000 long keys fits
	allUsersURI = "http://acs.amazonaws.com/groups/global/AllUsers"
)

// Options configures the handler.
type Options struct {
	// Region is reported to clients (any signed region is accepted).
	Region string
	// Domain enables virtual-host addressing (bucket.domain); empty disables it.
	Domain string
}

// Handler serves the S3 API.
type Handler struct {
	store  storage.Store
	auth   *auth.Verifier
	region string
	domain string
}

func New(store storage.Store, verifier *auth.Verifier, o Options) *Handler {
	return &Handler{store: store, auth: verifier, region: o.Region, domain: strings.ToLower(strings.Trim(o.Domain, "."))}
}

// request is the per-request context.
type request struct {
	w           http.ResponseWriter
	r           *http.Request
	query       url.Values
	bucket, key string
	auth        auth.Result
	id          string
}

func (q *request) has(name string) bool { _, ok := q.query[name]; return ok }
func (q *request) q(name string) string { return q.query.Get(name) }
func (q *request) header(name string) string {
	return q.r.Header.Get(name)
}

func requestID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return strings.ToUpper(hex.EncodeToString(b[:]))
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == HealthPath && (r.Method == http.MethodGet || r.Method == http.MethodHead) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = io.WriteString(w, "ok\n")
		return
	}
	q := &request{w: w, r: r, id: requestID()}
	w.Header().Set("Server", "objex")
	w.Header().Set("X-Amz-Request-Id", q.id)
	var err error
	if q.bucket, q.key, err = h.splitPath(r); err != nil {
		h.writeError(q, s3err.InvalidArgument.WithMessage("Invalid URI encoding"))
		return
	}
	// CORS headers go on every response to a matching origin, errors included,
	// so they are set before anything is written.
	if origin := r.Header.Get("Origin"); origin != "" && q.bucket != "" && r.Method != http.MethodOptions {
		if b, berr := h.store.GetBucket(r.Context(), q.bucket); berr == nil {
			applyCORS(b.CORS, origin, r.Method, w.Header())
		}
	}
	if err := h.serve(q); err != nil {
		h.writeError(q, err)
	}
}

func (h *Handler) serve(q *request) error {
	var err error
	if q.r.Method == http.MethodOptions {
		return h.preflight(q)
	}
	if q.query, err = url.ParseQuery(q.r.URL.RawQuery); err != nil {
		return s3err.InvalidArgument.WithMessage("Invalid query string")
	}
	if q.auth, err = h.auth.Authenticate(q.r, time.Now()); err != nil {
		return err
	}
	op, err := route(q)
	if err != nil {
		return err
	}
	if err := h.authorize(q, op); err != nil {
		return err
	}
	return op.run(h, q)
}

// splitPath extracts (bucket, key) for path-style and virtual-host requests.
func (h *Handler) splitPath(r *http.Request) (string, string, error) {
	path := r.URL.EscapedPath()
	if h.domain != "" {
		host := strings.ToLower(r.Host)
		if hh, _, err := net.SplitHostPort(host); err == nil {
			host = hh
		}
		if b, ok := strings.CutSuffix(host, "."+h.domain); ok && b != "" {
			key, err := url.PathUnescape(strings.TrimPrefix(path, "/"))
			return b, key, err
		}
	}
	rawBucket, rawKey, _ := strings.Cut(strings.TrimPrefix(path, "/"), "/")
	bucket, err := url.PathUnescape(rawBucket)
	if err != nil {
		return "", "", err
	}
	key, err := url.PathUnescape(rawKey)
	return bucket, key, err
}

func (h *Handler) authorize(q *request, op operation) error {
	key := q.auth.Key
	if key == nil {
		if op.publicRead && q.bucket != "" {
			b, err := h.store.GetBucket(q.r.Context(), q.bucket)
			if err != nil {
				return err
			}
			if b.PublicRead {
				return nil
			}
		}
		return s3err.AccessDenied
	}
	write := q.r.Method != http.MethodGet && q.r.Method != http.MethodHead
	if write && key.ReadOnly {
		return s3err.AccessDenied
	}
	if q.bucket != "" && !key.CanAccess(q.bucket) {
		return s3err.AccessDenied
	}
	if op.srcBucket != "" && !key.CanAccess(op.srcBucket) {
		return s3err.AccessDenied
	}
	return nil
}

// ---------------------------------------------------------------------------
// Responses
// ---------------------------------------------------------------------------

func (h *Handler) writeError(q *request, err error) {
	e := s3err.As(err)
	if e.Status >= 500 {
		slog.Error("request failed", "method", q.r.Method, "path", q.r.URL.Path, "err", err)
	}
	if e.Status == http.StatusNotModified || q.r.Method == http.MethodHead {
		q.w.WriteHeader(e.Status)
		return
	}
	writeXML(q.w, e.Status, struct {
		XMLName   xml.Name `xml:"Error"`
		Code      string
		Message   string
		Resource  string
		RequestID string `xml:"RequestId"`
	}{Code: e.Code, Message: e.Message, Resource: q.r.URL.Path, RequestID: q.id})
}

func writeXML(w http.ResponseWriter, status int, v any) {
	body, err := xml.Marshal(v)
	if err != nil {
		slog.Error("xml encode", "err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("Content-Length", strconv.Itoa(len(xml.Header)+len(body)))
	w.WriteHeader(status)
	_, _ = io.WriteString(w, xml.Header)
	_, _ = w.Write(body)
}

func ok(w http.ResponseWriter, status int) error {
	w.WriteHeader(status)
	return nil
}

type owner struct {
	ID          string
	DisplayName string
}

var theOwner = owner{ID: "objex", DisplayName: "objex"}

func iso8601(t time.Time) string { return t.UTC().Format("2006-01-02T15:04:05.000Z") }
func quote(etag string) string   { return `"` + etag + `"` }
func setChecksum(h http.Header, c *checksum.Checksum) {
	if c != nil {
		h.Set(c.Algo.Header(), c.Value)
		h.Set("X-Amz-Checksum-Type", string(c.Type()))
	}
}

// ---------------------------------------------------------------------------
// Request bodies
// ---------------------------------------------------------------------------

// body returns the request body, decoding aws-chunked when used.
func (q *request) body() (io.Reader, *auth.ChunkedReader) {
	if q.auth.Mode == auth.Streaming {
		c := auth.NewChunkedReader(q.r.Body, q.auth.Chunks, q.auth.Trailer)
		return c, c
	}
	return q.r.Body, nil
}

// expect builds the upload expectations from the request headers.
func (q *request) expect(chunked *auth.ChunkedReader) (storage.Expect, error) {
	e := storage.Expect{Size: q.r.ContentLength}
	if v := q.header("Content-MD5"); v != "" {
		raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(v))
		if err != nil || len(raw) != md5.Size {
			return e, s3err.InvalidDigest
		}
		e.ContentMD5 = raw
	}
	for _, a := range checksum.All {
		if v := q.header(a.Header()); v != "" {
			if e.Checksum != nil {
				return e, s3err.InvalidRequest.WithMessage("Expecting a single x-amz-checksum- header. Multiple checksum Types are not allowed.")
			}
			c := checksum.Checksum{Algo: a, Value: strings.TrimSpace(v)}
			if !c.Valid() || strings.Contains(c.Value, "-") {
				return e, s3err.InvalidRequest.WithMessagef("Value for %s header is invalid.", a.Header())
			}
			e.Checksum = &c
		}
	}
	named := q.header("X-Amz-Sdk-Checksum-Algorithm")
	if named == "" {
		named = q.header("X-Amz-Checksum-Algorithm")
	}
	if named != "" {
		a, ok := checksum.Parse(named)
		if !ok {
			return e, s3err.InvalidRequest.WithMessage("Invalid checksum algorithm")
		}
		e.Algo = a
	}
	if t := q.header("X-Amz-Trailer"); t != "" {
		a, ok := checksum.FromHeader(t)
		if !ok {
			return e, s3err.InvalidRequest.WithMessage("Unsupported trailer " + t)
		}
		e.Algo = a
	}
	if e.Checksum != nil && e.Algo != "" && e.Checksum.Algo != e.Algo {
		return e, s3err.InvalidRequest.WithMessage("Value for x-amz-checksum-algorithm header is invalid.")
	}
	switch q.auth.Mode {
	case auth.Streaming:
		n, err := strconv.ParseInt(q.header("X-Amz-Decoded-Content-Length"), 10, 64)
		if err != nil || n < 0 {
			return e, s3err.MissingContentLength
		}
		e.Size = n
		if chunked != nil {
			e.Trailer = chunked.TrailingChecksum
		}
	case auth.SHA256:
		e.SHA256 = q.auth.SHA256
	}
	if e.Size < 0 && len(q.r.TransferEncoding) == 0 {
		return e, s3err.MissingContentLength
	}
	if e.Size > storage.MaxPutSize {
		return e, s3err.EntityTooLarge
	}
	return e, nil
}

// readXML reads a small XML request body, verifying its SHA-256 and Content-MD5.
func (q *request) readXML(v any) error {
	data, err := q.readBody()
	if err != nil {
		return err
	}
	if err := xml.Unmarshal(data, v); err != nil {
		return s3err.MalformedXML
	}
	return nil
}

func (q *request) readBody() ([]byte, error) {
	body, _ := q.body()
	data, err := io.ReadAll(io.LimitReader(body, maxXMLBody+1))
	if err != nil {
		var e *s3err.Error
		if errors.As(err, &e) {
			return nil, e
		}
		return nil, s3err.IncompleteBody
	}
	if len(data) > maxXMLBody {
		return nil, s3err.InvalidRequest.WithMessage("Request body is too large")
	}
	if q.auth.Mode == auth.SHA256 {
		if sum := sha256.Sum256(data); string(sum[:]) != string(q.auth.SHA256) {
			return nil, s3err.XAmzContentSHA256Mismatch
		}
	}
	if v := q.header("Content-MD5"); v != "" {
		want, err := base64.StdEncoding.DecodeString(strings.TrimSpace(v))
		if sum := md5.Sum(data); err != nil || string(sum[:]) != string(want) {
			return nil, s3err.BadDigest
		}
	}
	return data, nil
}

func metadataFromHeaders(h http.Header) (storage.Metadata, error) {
	m := storage.Metadata{
		ContentType: h.Get("Content-Type"), ContentDisposition: h.Get("Content-Disposition"),
		ContentLanguage: h.Get("Content-Language"), CacheControl: h.Get("Cache-Control"), Expires: h.Get("Expires"),
	}
	// aws-chunked is a transfer detail, not a property of the object.
	var enc []string
	for _, e := range strings.Split(h.Get("Content-Encoding"), ",") {
		if e = strings.TrimSpace(e); e != "" && !strings.EqualFold(e, "aws-chunked") {
			enc = append(enc, e)
		}
	}
	m.ContentEncoding = strings.Join(enc, ",")
	size := 0
	for name, values := range h {
		if n, ok := strings.CutPrefix(strings.ToLower(name), "x-amz-meta-"); ok {
			if m.User == nil {
				m.User = map[string]string{}
			}
			v := strings.Join(values, ",")
			m.User[n] = v
			size += len(n) + len(v)
		}
	}
	if size > storage.MaxUserMeta {
		return m, s3err.MetadataTooLarge
	}
	return m, nil
}

func parseMaxKeys(v, name string) (int, error) {
	if v == "" {
		return 1000, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return 0, s3err.InvalidArgument.WithMessagef("Provided %s not an integer or within integer range", name)
	}
	return min(n, 1000), nil
}
