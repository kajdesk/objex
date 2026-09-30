package s3

import (
	"encoding/xml"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/kajdesk/objex/internal/checksum"
	"github.com/kajdesk/objex/internal/s3err"
	"github.com/kajdesk/objex/internal/storage"
)

func parseHTTPDate(v string) time.Time {
	t, err := http.ParseTime(strings.TrimSpace(v))
	if err != nil {
		return time.Time{}
	}
	return t
}

func readConditions(h http.Header, prefix string) storage.ReadConditions {
	return storage.ReadConditions{
		IfMatch: h.Get(prefix + "If-Match"), IfNoneMatch: h.Get(prefix + "If-None-Match"),
		IfModifiedSince: parseHTTPDate(h.Get(prefix + "If-Modified-Since")), IfUnmodifiedSince: parseHTTPDate(h.Get(prefix + "If-Unmodified-Since")),
	}
}

func writeConditions(h http.Header) storage.WriteConditions {
	return storage.WriteConditions{IfMatch: h.Get("If-Match"), IfNoneMatch: h.Get("If-None-Match")}
}

// objectHeaders describes an object: metadata (with response-* overrides),
// ETag and dates.
func (q *request) objectHeaders(o storage.Object) {
	h, m := q.w.Header(), o.Metadata
	pick := func(override, stored string) string {
		if v := q.q(override); v != "" {
			return v
		}
		return stored
	}
	ct := pick("response-content-type", m.ContentType)
	if ct == "" {
		ct = "application/octet-stream"
	}
	h.Set("Content-Type", ct)
	for _, f := range []struct{ header, override, value string }{
		{"Content-Encoding", "response-content-encoding", m.ContentEncoding},
		{"Content-Disposition", "response-content-disposition", m.ContentDisposition},
		{"Content-Language", "response-content-language", m.ContentLanguage},
		{"Cache-Control", "response-cache-control", m.CacheControl},
		{"Expires", "response-expires", m.Expires},
	} {
		if v := pick(f.override, f.value); v != "" {
			h.Set(f.header, v)
		}
	}
	for k, v := range m.User {
		h.Set("X-Amz-Meta-"+k, v)
	}
	h.Set("ETag", quote(o.ETag))
	h.Set("Last-Modified", o.LastModified.UTC().Format(http.TimeFormat))
	h.Set("Accept-Ranges", "bytes")
	h.Set("X-Amz-Storage-Class", "STANDARD")
	if len(o.Parts) > 0 {
		h.Set("X-Amz-Mp-Parts-Count", strconv.Itoa(len(o.Parts)))
	}
}

// parseRange parses a single-range Range header. Unsupported or malformed
// ranges are ignored (ok false), as S3 does.
func parseRange(v string) (first, last int64, suffix bool, ok bool) {
	spec, found := strings.CutPrefix(strings.TrimSpace(v), "bytes=")
	if !found || strings.Contains(spec, ",") {
		return
	}
	a, b, found := strings.Cut(spec, "-")
	if !found {
		return
	}
	a, b = strings.TrimSpace(a), strings.TrimSpace(b)
	if a == "" {
		n, err := strconv.ParseInt(b, 10, 64)
		return 0, n, true, err == nil && n >= 0
	}
	f, err := strconv.ParseInt(a, 10, 64)
	if err != nil || f < 0 {
		return
	}
	if b == "" {
		return f, -1, false, true
	}
	l, err := strconv.ParseInt(b, 10, 64)
	if err != nil || l < f {
		return
	}
	return f, l, false, true
}

// resolveRange resolves a parsed range against an object size to (start, length).
func resolveRange(first, last int64, suffix bool, size int64) (int64, int64, error) {
	if suffix {
		if last == 0 || size == 0 {
			return 0, 0, s3err.InvalidRange
		}
		n := min(last, size)
		return size - n, n, nil
	}
	if first >= size {
		return 0, 0, s3err.InvalidRange
	}
	if last < 0 || last >= size {
		last = size - 1
	}
	return first, last - first + 1, nil
}

func (h *Handler) getObject(q *request) error {
	head := q.r.Method == http.MethodHead
	var part int
	if v := q.q("partNumber"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > storage.MaxPartNumber {
			return s3err.InvalidArgument.WithMessage("Part number must be an integer between 1 and 10000, inclusive")
		}
		if q.header("Range") != "" {
			return s3err.InvalidRequest.WithMessage("Cannot specify both Range header and partNumber query parameter")
		}
		part = n
	}
	o, reader, err := h.store.OpenObject(q.r.Context(), q.bucket, q.key)
	if err != nil {
		return err
	}
	defer reader.Close()
	if err := readConditions(q.r.Header, "").Check(o, false); err != nil {
		if errors.Is(err, s3err.NotModified) {
			q.w.Header().Set("ETag", quote(o.ETag))
			q.w.Header().Set("Last-Modified", o.LastModified.UTC().Format(http.TimeFormat))
		}
		return err
	}
	start, length, partial := int64(0), o.Size, false
	switch {
	case part > 0 && len(o.Parts) == 0:
		if part != 1 {
			return s3err.InvalidPartNumber
		}
	case part > 0:
		s, n, found := reader.PartRange(part)
		if !found {
			return s3err.InvalidPartNumber
		}
		start, length, partial = s, n, true
	default:
		if first, last, suffix, found := parseRange(q.header("Range")); found {
			s, n, err := resolveRange(first, last, suffix, o.Size)
			if err != nil {
				q.w.Header().Set("Content-Range", "bytes */"+strconv.FormatInt(o.Size, 10))
				return err
			}
			start, length, partial = s, n, true
		}
	}
	// Fail before any headers go out if the data is missing or truncated.
	if !head && length > 0 {
		if err := reader.Check(start); err != nil {
			return err
		}
	}
	q.objectHeaders(o)
	hd := q.w.Header()
	hd.Set("Content-Length", strconv.FormatInt(length, 10))
	if !partial && strings.EqualFold(q.header("X-Amz-Checksum-Mode"), "ENABLED") {
		setChecksum(hd, o.Checksum)
	}
	status := http.StatusOK
	if partial {
		status = http.StatusPartialContent
		hd.Set("Content-Range", "bytes "+strconv.FormatInt(start, 10)+"-"+strconv.FormatInt(start+length-1, 10)+"/"+strconv.FormatInt(o.Size, 10))
	}
	q.w.WriteHeader(status)
	if head || length == 0 {
		return nil
	}
	if err := reader.CopyRange(q.w, start, length); err != nil {
		// Headers are gone; the only honest signal left is to cut the
		// connection so the client sees a truncated transfer.
		if q.r.Context().Err() == nil {
			slog.Error("download aborted", "bucket", q.bucket, "key", q.key, "err", err)
		}
		panic(http.ErrAbortHandler)
	}
	return nil
}

func (h *Handler) putObject(q *request) error {
	body, chunked := q.body()
	e, err := q.expect(chunked)
	if err != nil {
		return err
	}
	meta, err := metadataFromHeaders(q.r.Header)
	if err != nil {
		return err
	}
	o, err := h.store.PutObject(q.r.Context(), q.bucket, q.key, body, storage.PutOptions{Metadata: meta, Expect: e, Cond: writeConditions(q.r.Header)})
	if err != nil {
		return err
	}
	q.w.Header().Set("ETag", quote(o.ETag))
	setChecksum(q.w.Header(), o.Checksum)
	return ok(q.w, http.StatusOK)
}

func (h *Handler) copyObject(q *request) error {
	srcBucket, srcKey, err := parseCopySource(q.header("X-Amz-Copy-Source"))
	if err != nil {
		return err
	}
	opts := storage.CopyOptions{SourceCond: readConditions(q.r.Header, "X-Amz-Copy-Source-"), Cond: writeConditions(q.r.Header)}
	switch strings.ToUpper(q.header("X-Amz-Metadata-Directive")) {
	case "", "COPY":
	case "REPLACE":
		m, err := metadataFromHeaders(q.r.Header)
		if err != nil {
			return err
		}
		opts.ReplaceMetadata = &m
	default:
		return s3err.InvalidArgument.WithMessage("Unknown metadata directive.")
	}
	o, err := h.store.CopyObject(q.r.Context(), srcBucket, srcKey, q.bucket, q.key, opts)
	if err != nil {
		return err
	}
	writeXML(q.w, http.StatusOK, copyResult("CopyObjectResult", o.LastModified, o.ETag, o.Checksum))
	return nil
}

type copyResultXML struct {
	XMLName      xml.Name
	XMLNS        string `xml:"xmlns,attr"`
	LastModified string
	ETag         string
	Checksums    []checksumXML
	ChecksumType string `xml:",omitempty"`
}

// checksumXML renders <ChecksumCRC32>value</ChecksumCRC32> and friends.
type checksumXML struct {
	XMLName xml.Name
	Value   string `xml:",chardata"`
}

func checksumElems(c *checksum.Checksum) []checksumXML {
	if c == nil {
		return nil
	}
	return []checksumXML{{XMLName: xml.Name{Local: c.Algo.XMLTag()}, Value: c.Value}}
}

func copyResult(name string, modified time.Time, etag string, c *checksum.Checksum) copyResultXML {
	r := copyResultXML{XMLName: xml.Name{Local: name}, XMLNS: xmlns, LastModified: iso8601(modified), ETag: quote(etag), Checksums: checksumElems(c)}
	if c != nil && name == "CopyObjectResult" {
		r.ChecksumType = string(c.Type())
	}
	return r
}

func (h *Handler) deleteObject(q *request) error {
	// S3 does not reveal whether the key existed.
	results, err := h.store.DeleteObjects(q.r.Context(), q.bucket, []string{q.key})
	if err != nil {
		return err
	}
	if results[0] != nil {
		return results[0]
	}
	return ok(q.w, http.StatusNoContent)
}

func (h *Handler) getObjectACL(q *request) error {
	b, err := h.store.GetBucket(q.r.Context(), q.bucket)
	if err != nil {
		return err
	}
	if _, err := h.store.HeadObject(q.r.Context(), q.bucket, q.key); err != nil {
		return err
	}
	writeXML(q.w, http.StatusOK, aclXML(b.PublicRead))
	return nil
}

// putObjectACL: object ACLs are not stored (access is per bucket), so only
// private is accepted.
func (h *Handler) putObjectACL(q *request) error {
	if _, err := h.store.HeadObject(q.r.Context(), q.bucket, q.key); err != nil {
		return err
	}
	switch q.header("X-Amz-Acl") {
	case "", "private", "bucket-owner-full-control":
		return ok(q.w, http.StatusOK)
	}
	return s3err.NotImplemented.WithMessage("Per-object ACLs are not supported; use a public-read bucket")
}

func (h *Handler) getTagging(q *request) error {
	if _, err := h.store.HeadObject(q.r.Context(), q.bucket, q.key); err != nil {
		return err
	}
	writeXML(q.w, http.StatusOK, struct {
		XMLName xml.Name `xml:"Tagging"`
		XMLNS   string   `xml:"xmlns,attr"`
		TagSet  struct{} `xml:"TagSet"`
	}{XMLNS: xmlns})
	return nil
}
