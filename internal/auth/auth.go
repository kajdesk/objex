// Package auth verifies AWS Signature Version 4: header authentication,
// presigned URLs, and the chained signatures of streaming (aws-chunked) uploads.
package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/kajdesk/objex/internal/config"
	"github.com/kajdesk/objex/internal/s3err"
)

const (
	Algorithm                = "AWS4-HMAC-SHA256"
	UnsignedPayload          = "UNSIGNED-PAYLOAD"
	StreamingSigned          = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
	StreamingSignedTrailer   = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"
	StreamingUnsignedTrailer = "STREAMING-UNSIGNED-PAYLOAD-TRAILER"
	EmptySHA256              = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

	maxSkew    = 15 * time.Minute
	maxPresign = 7 * 24 * time.Hour
)

// PayloadMode is how a request body is protected.
type PayloadMode int

const (
	// Unsigned: UNSIGNED-PAYLOAD, presigned URLs and anonymous requests.
	Unsigned PayloadMode = iota
	// SHA256: the body must hash to Result.SHA256.
	SHA256
	// Streaming: aws-chunked encoding; Result.Chunks verifies chunk signatures
	// when they are signed.
	Streaming
)

// Result of authenticating a request.
type Result struct {
	// Key is nil for anonymous requests.
	Key    *config.Key
	Mode   PayloadMode
	SHA256 []byte
	// Chunks is set for signed streaming uploads.
	Chunks *ChunkSigner
	// Trailer reports whether a streaming body ends with trailing headers.
	Trailer bool
}

// Verifier authenticates requests against a key set that can be replaced at
// any time (hot reload).
type Verifier struct {
	keys atomic.Pointer[map[string]config.Key]
}

func New(keys []config.Key) *Verifier {
	v := &Verifier{}
	v.SetKeys(keys)
	return v
}

// SetKeys atomically replaces the key set.
func (v *Verifier) SetKeys(keys []config.Key) {
	m := make(map[string]config.Key, len(keys))
	for _, k := range keys {
		m[k.AccessKey] = k
	}
	v.keys.Store(&m)
}

func (v *Verifier) key(access string) (config.Key, bool) {
	k, ok := (*v.keys.Load())[access]
	return k, ok
}

// Authenticate verifies a request. Requests without credentials are anonymous
// (Result.Key is nil); whether that is allowed is up to the caller.
func (v *Verifier) Authenticate(req *http.Request, now time.Time) (Result, error) {
	now = now.UTC()
	query := ParseQuery(req.URL.RawQuery)
	if authz := req.Header.Get("Authorization"); authz != "" {
		return v.header(req, query, authz, now)
	}
	if first(query, "X-Amz-Algorithm") != "" || first(query, "X-Amz-Credential") != "" {
		return v.presigned(req, query, now)
	}
	if first(query, "AWSAccessKeyId") != "" {
		return Result{}, s3err.InvalidRequest.WithMessage("Signature Version 2 is not supported. Please use AWS4-HMAC-SHA256.")
	}
	return Result{}, nil
}

type signed struct {
	access, date, region, service string
	headers                       []string
	signature, amzDate, payload   string
	presigned                     bool
}

func (v *Verifier) header(req *http.Request, query []pair, authz string, now time.Time) (Result, error) {
	rest, ok := strings.CutPrefix(authz, Algorithm)
	if !ok {
		return Result{}, s3err.InvalidRequest.WithMessage("Unsupported authorization type. Please use AWS4-HMAC-SHA256.")
	}
	var credential, signedHeaders, signature string
	for _, field := range strings.Split(rest, ",") {
		k, val, ok := strings.Cut(strings.TrimSpace(field), "=")
		if !ok {
			return Result{}, s3err.AuthorizationHeaderMalformed
		}
		switch strings.TrimSpace(k) {
		case "Credential":
			credential = strings.TrimSpace(val)
		case "SignedHeaders":
			signedHeaders = strings.TrimSpace(val)
		case "Signature":
			signature = strings.TrimSpace(val)
		}
	}
	if credential == "" || signedHeaders == "" || signature == "" {
		return Result{}, s3err.AuthorizationHeaderMalformed
	}
	amzDate := req.Header.Get("X-Amz-Date")
	if amzDate == "" {
		t, err := http.ParseTime(req.Header.Get("Date"))
		if err != nil {
			return Result{}, s3err.AccessDenied.WithMessage("AWS authentication requires a valid Date or x-amz-date header")
		}
		amzDate = t.UTC().Format("20060102T150405Z")
	}
	t, err := time.Parse("20060102T150405Z", amzDate)
	if err != nil {
		return Result{}, s3err.AccessDenied.WithMessage("Invalid x-amz-date")
	}
	if d := now.Sub(t); d >= maxSkew || d <= -maxSkew {
		return Result{}, s3err.RequestTimeTooSkewed
	}
	payload := req.Header.Get("X-Amz-Content-Sha256")
	if payload == "" {
		payload = UnsignedPayload
	}
	s, err := parseCredential(credential, s3err.AuthorizationHeaderMalformed)
	if err != nil {
		return Result{}, err
	}
	s.headers, s.signature, s.amzDate, s.payload = strings.Split(signedHeaders, ";"), signature, amzDate, payload
	return v.verify(req, query, s)
}

func (v *Verifier) presigned(req *http.Request, query []pair, now time.Time) (Result, error) {
	missing := s3err.AuthorizationQueryParamsError.WithMessage("Query-string authentication version 4 requires the X-Amz-Algorithm, X-Amz-Credential, X-Amz-Signature, X-Amz-Date, X-Amz-SignedHeaders, and X-Amz-Expires parameters.")
	if first(query, "X-Amz-Algorithm") != Algorithm {
		return Result{}, s3err.AuthorizationQueryParamsError.WithMessage("X-Amz-Algorithm only supports \"AWS4-HMAC-SHA256\"")
	}
	credential, amzDate := first(query, "X-Amz-Credential"), first(query, "X-Amz-Date")
	expiresText, signedHeaders := first(query, "X-Amz-Expires"), first(query, "X-Amz-SignedHeaders")
	signature := first(query, "X-Amz-Signature")
	if credential == "" || amzDate == "" || expiresText == "" || signedHeaders == "" || signature == "" {
		return Result{}, missing
	}
	t, err := time.Parse("20060102T150405Z", amzDate)
	if err != nil {
		return Result{}, s3err.AuthorizationQueryParamsError.WithMessage("Invalid X-Amz-Date")
	}
	secs, err := strconv.ParseInt(expiresText, 10, 64)
	if err != nil || secs < 0 || time.Duration(secs)*time.Second > maxPresign {
		return Result{}, s3err.AuthorizationQueryParamsError.WithMessage("X-Amz-Expires must be less than a week (in seconds) that is 604800")
	}
	if t.Sub(now) > maxSkew {
		return Result{}, s3err.AccessDenied.WithMessage("Request is not valid yet")
	}
	if now.After(t.Add(time.Duration(secs) * time.Second)) {
		return Result{}, s3err.AccessDenied.WithMessage("Request has expired")
	}
	payload := first(query, "X-Amz-Content-Sha256")
	if payload == "" {
		payload = UnsignedPayload
	}
	s, err := parseCredential(credential, s3err.AuthorizationQueryParamsError)
	if err != nil {
		return Result{}, err
	}
	s.headers, s.signature, s.amzDate, s.payload, s.presigned = strings.Split(signedHeaders, ";"), signature, amzDate, payload, true
	return v.verify(req, query, s)
}

// parseCredential splits AKID/20130524/us-east-1/s3/aws4_request. Access keys
// cannot contain '/', so the fields are taken from the right.
func parseCredential(c string, malformed *s3err.Error) (signed, error) {
	parts := strings.Split(c, "/")
	n := len(parts)
	if n < 5 || parts[n-1] != "aws4_request" || len(parts[n-4]) != 8 {
		return signed{}, malformed
	}
	return signed{access: strings.Join(parts[:n-4], "/"), date: parts[n-4], region: parts[n-3], service: parts[n-2]}, nil
}

func (v *Verifier) verify(req *http.Request, query []pair, s signed) (Result, error) {
	key, ok := v.key(s.access)
	if !ok {
		return Result{}, s3err.InvalidAccessKeyID
	}
	malformed := s3err.AuthorizationHeaderMalformed
	if s.presigned {
		malformed = s3err.AuthorizationQueryParamsError
	}
	if !strings.HasPrefix(s.amzDate, s.date) {
		return Result{}, malformed.WithMessage("Credential date does not match the request date")
	}
	if s.service != "s3" {
		return Result{}, malformed.WithMessage("Unsupported service " + s.service)
	}
	if !contains(s.headers, "host") {
		return Result{}, s3err.AccessDenied.WithMessage("Host must be a signed header")
	}
	headers, err := canonicalHeaders(req, s.headers)
	if err != nil {
		return Result{}, err
	}
	skey := SigningKey(key.SecretKey, s.date, s.region, s.service)
	scope := s.date + "/" + s.region + "/" + s.service + "/aws4_request"
	cquery := CanonicalQuery(query, s.presigned)
	matched := false
	for _, uri := range canonicalURIs(req) {
		creq := req.Method + "\n" + uri + "\n" + cquery + "\n" + headers + "\n" + strings.Join(s.headers, ";") + "\n" + s.payload
		sts := Algorithm + "\n" + s.amzDate + "\n" + scope + "\n" + hexSHA256([]byte(creq))
		if hmac.Equal([]byte(hex.EncodeToString(hmacSHA256(skey, []byte(sts)))), []byte(s.signature)) {
			matched = true
			break
		}
	}
	if !matched {
		return Result{}, s3err.SignatureDoesNotMatch
	}

	r := Result{Key: &key}
	switch s.payload {
	case UnsignedPayload:
		r.Mode = Unsigned
	case StreamingUnsignedTrailer:
		r.Mode, r.Trailer = Streaming, true
	case StreamingSigned, StreamingSignedTrailer:
		r.Mode, r.Trailer = Streaming, s.payload == StreamingSignedTrailer
		r.Chunks = &ChunkSigner{key: skey, amzDate: s.amzDate, scope: scope, prev: s.signature}
	default:
		if strings.HasPrefix(s.payload, "STREAMING-") {
			return Result{}, s3err.NotImplemented.WithMessage("Unsupported streaming payload " + s.payload)
		}
		digest, err := hex.DecodeString(s.payload)
		if err != nil || len(digest) != sha256.Size {
			return Result{}, s3err.InvalidArgument.WithMessage("x-amz-content-sha256 must be UNSIGNED-PAYLOAD, a streaming mode, or a valid sha256 value.")
		}
		r.Mode, r.SHA256 = SHA256, digest
	}
	return r, nil
}

func canonicalHeaders(req *http.Request, names []string) (string, error) {
	var b strings.Builder
	for _, name := range names {
		var value string
		if name == "host" {
			value = req.Host
			if value == "" {
				value = req.URL.Host
			}
		} else {
			values := req.Header.Values(name)
			if name == "content-length" && len(values) == 0 && req.ContentLength >= 0 {
				// net/http moves Content-Length out of the header map.
				values = []string{strconv.FormatInt(req.ContentLength, 10)}
			}
			if name == "transfer-encoding" && len(values) == 0 && len(req.TransferEncoding) > 0 {
				values = req.TransferEncoding
			}
			if len(values) == 0 {
				return "", s3err.AccessDenied.WithMessage("Signed header " + name + " is missing")
			}
			for i, v := range values {
				values[i] = strings.Join(strings.Fields(v), " ")
			}
			value = strings.Join(values, ",")
		}
		b.WriteString(name)
		b.WriteByte(':')
		b.WriteString(strings.Join(strings.Fields(value), " "))
		b.WriteByte('\n')
	}
	return b.String(), nil
}

// canonicalURIs returns the canonical URIs to try: the path re-encoded from its
// decoded form (what AWS SDKs sign) and the path exactly as received (for
// clients that encode differently).
func canonicalURIs(req *http.Request) []string {
	raw := req.URL.EscapedPath()
	if raw == "" {
		raw = "/"
	}
	out := make([]string, 0, 2)
	if decoded, err := url.PathUnescape(raw); err == nil {
		out = append(out, EncodePath(decoded))
	}
	if len(out) == 0 || out[0] != raw {
		out = append(out, raw)
	}
	return out
}

type pair struct{ k, v string }

// ParseQuery parses a raw query string, percent-decoding without turning '+'
// into a space (SigV4 canonicalization treats '+' literally).
func ParseQuery(raw string) []pair {
	var out []pair
	for _, part := range strings.Split(raw, "&") {
		if part == "" {
			continue
		}
		k, v, _ := strings.Cut(part, "=")
		if dk, err := url.PathUnescape(k); err == nil {
			k = dk
		}
		if dv, err := url.PathUnescape(v); err == nil {
			v = dv
		}
		out = append(out, pair{k, v})
	}
	return out
}

func first(q []pair, name string) string {
	for _, p := range q {
		if p.k == name {
			return p.v
		}
	}
	return ""
}

// CanonicalQuery returns the SigV4 canonical query string, optionally without
// X-Amz-Signature (presigned verification).
func CanonicalQuery(q []pair, skipSignature bool) string {
	enc := make([]pair, 0, len(q))
	for _, p := range q {
		if skipSignature && p.k == "X-Amz-Signature" {
			continue
		}
		enc = append(enc, pair{Encode(p.k), Encode(p.v)})
	}
	sort.Slice(enc, func(i, j int) bool {
		if enc[i].k != enc[j].k {
			return enc[i].k < enc[j].k
		}
		return enc[i].v < enc[j].v
	})
	parts := make([]string, len(enc))
	for i, p := range enc {
		parts[i] = p.k + "=" + p.v
	}
	return strings.Join(parts, "&")
}

const upperhex = "0123456789ABCDEF"

func encode(s string, keepSlash bool) string {
	var b strings.Builder
	b.Grow(len(s) + 16)
	for i := 0; i < len(s); i++ {
		c := s[i]
		if ('A' <= c && c <= 'Z') || ('a' <= c && c <= 'z') || ('0' <= c && c <= '9') || c == '-' || c == '_' || c == '.' || c == '~' || (keepSlash && c == '/') {
			b.WriteByte(c)
		} else {
			b.WriteByte('%')
			b.WriteByte(upperhex[c>>4])
			b.WriteByte(upperhex[c&15])
		}
	}
	return b.String()
}

// Encode URI-encodes per SigV4: everything but unreserved characters.
func Encode(s string) string { return encode(s, false) }

// EncodePath is Encode, keeping '/' (object key paths).
func EncodePath(s string) string { return encode(s, true) }

// SigningKey derives the SigV4 signing key.
func SigningKey(secret, date, region, service string) []byte {
	k := hmacSHA256([]byte("AWS4"+secret), []byte(date))
	k = hmacSHA256(k, []byte(region))
	k = hmacSHA256(k, []byte(service))
	return hmacSHA256(k, []byte("aws4_request"))
}

func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	_, _ = h.Write(data)
	return h.Sum(nil)
}

func hexSHA256(data []byte) string {
	s := sha256.Sum256(data)
	return hex.EncodeToString(s[:])
}

func contains(values []string, value string) bool {
	for _, v := range values {
		if v == value {
			return true
		}
	}
	return false
}

// ChunkSigner verifies the chained signatures of aws-chunked uploads.
type ChunkSigner struct {
	key            []byte
	amzDate, scope string
	prev           string
}

// VerifyChunk checks the signature of the next chunk, given its SHA-256.
func (c *ChunkSigner) VerifyChunk(chunkSHA256 []byte, signature string) error {
	sts := "AWS4-HMAC-SHA256-PAYLOAD\n" + c.amzDate + "\n" + c.scope + "\n" + c.prev + "\n" + EmptySHA256 + "\n" + hex.EncodeToString(chunkSHA256)
	return c.advance(sts, signature)
}

// VerifyTrailer checks the signature over the trailing headers, given as
// canonical "name:value\n" lines.
func (c *ChunkSigner) VerifyTrailer(canonical, signature string) error {
	sts := "AWS4-HMAC-SHA256-TRAILER\n" + c.amzDate + "\n" + c.scope + "\n" + c.prev + "\n" + hexSHA256([]byte(canonical))
	return c.advance(sts, signature)
}

func (c *ChunkSigner) advance(sts, signature string) error {
	expected := hex.EncodeToString(hmacSHA256(c.key, []byte(sts)))
	if !hmac.Equal([]byte(expected), []byte(signature)) {
		return s3err.SignatureDoesNotMatch
	}
	c.prev = expected
	return nil
}
