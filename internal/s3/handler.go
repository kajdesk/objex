package s3

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/kajdesk/objex/internal/auth"
	"github.com/kajdesk/objex/internal/config"
	"github.com/kajdesk/objex/internal/storage"
)

const namespace = "http://s3.amazonaws.com/doc/2006-03-01/"

type Handler struct {
	store    storage.Store
	auth     *auth.Verifier
	region   string
	domain   string
	maxWrite int64
}

func New(store storage.Store, cfg config.Config) *Handler {
	return &Handler{
		store: store, auth: auth.New(cfg.Keys), region: cfg.Region,
		domain: strings.ToLower(strings.Trim(cfg.Domain, ".")), maxWrite: 5 << 30,
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	requestID := randomRequestID()
	w.Header().Set("Server", "objex")
	w.Header().Set("X-Amz-Request-Id", requestID)
	if r.URL.Path == "/_objex/health" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = io.WriteString(w, "ok\n")
		return
	}
	bucket, key, err := h.splitPath(r)
	if err != nil {
		h.writeError(w, r, requestID, apiError("InvalidArgument", http.StatusBadRequest, err.Error()))
		return
	}
	authResult, err := h.auth.Authenticate(r, time.Now())
	if err != nil {
		h.writeError(w, r, requestID, fromAuthError(err))
		return
	}
	if err := h.authorize(r.Context(), authResult.Key, r.Method, bucket, key); err != nil {
		h.writeError(w, r, requestID, err)
		return
	}
	if err := h.route(w, r, authResult, bucket, key); err != nil {
		h.writeError(w, r, requestID, err)
	}
}

func (h *Handler) authorize(ctx context.Context, keyConfig *config.Key, method, bucket, objectKey string) error {
	if keyConfig == nil {
		if bucket != "" && (method == http.MethodGet || method == http.MethodHead) {
			info, err := h.store.GetBucket(ctx, bucket)
			if err == nil && info.PublicRead {
				return nil
			}
		}
		return apiError("AccessDenied", http.StatusForbidden, "Access Denied")
	}
	if bucket != "" && !keyConfig.CanAccess(bucket) {
		return apiError("AccessDenied", http.StatusForbidden, "Access Denied")
	}
	if keyConfig.ReadOnly && method != http.MethodGet && method != http.MethodHead {
		return apiError("AccessDenied", http.StatusForbidden, "Access Denied")
	}
	return nil
}

func (h *Handler) route(w http.ResponseWriter, r *http.Request, result auth.Result, bucket, key string) error {
	query := r.URL.Query()
	if query.Has("uploads") || query.Has("uploadId") || query.Has("partNumber") || r.Header.Get("X-Amz-Copy-Source") != "" {
		return apiError("NotImplemented", http.StatusNotImplemented, "multipart upload and copy are not implemented in the Go milestone yet")
	}
	if bucket == "" {
		if r.Method != http.MethodGet {
			return methodNotAllowed()
		}
		return h.listBuckets(w, r)
	}
	if key == "" {
		switch r.Method {
		case http.MethodPut:
			return h.createBucket(w, r, bucket)
		case http.MethodHead:
			return h.headBucket(w, r, bucket)
		case http.MethodDelete:
			return h.deleteBucket(w, r, bucket)
		case http.MethodGet:
			if query.Has("location") {
				return h.bucketLocation(w, r, bucket)
			}
			return h.listObjects(w, r, bucket)
		default:
			return methodNotAllowed()
		}
	}
	switch r.Method {
	case http.MethodPut:
		return h.putObject(w, r, result, bucket, key)
	case http.MethodGet:
		return h.getObject(w, r, bucket, key, false)
	case http.MethodHead:
		return h.getObject(w, r, bucket, key, true)
	case http.MethodDelete:
		return h.deleteObject(w, r, bucket, key)
	default:
		return methodNotAllowed()
	}
}

func (h *Handler) listBuckets(w http.ResponseWriter, r *http.Request) error {
	buckets, err := h.store.ListBuckets(r.Context())
	if err != nil {
		return internalError(err)
	}
	type item struct {
		Name         string `xml:"Name"`
		CreationDate string `xml:"CreationDate"`
	}
	response := struct {
		XMLName xml.Name `xml:"ListAllMyBucketsResult"`
		XMLNS   string   `xml:"xmlns,attr"`
		Owner   owner    `xml:"Owner"`
		Buckets []item   `xml:"Buckets>Bucket"`
	}{XMLNS: namespace, Owner: defaultOwner()}
	for _, bucket := range buckets {
		response.Buckets = append(response.Buckets, item{Name: bucket.Name, CreationDate: iso8601(bucket.Created)})
	}
	writeXML(w, http.StatusOK, response)
	return nil
}

func (h *Handler) createBucket(w http.ResponseWriter, r *http.Request, bucket string) error {
	public := strings.EqualFold(r.Header.Get("X-Amz-Acl"), "public-read")
	if err := h.store.CreateBucket(r.Context(), bucket, public); err != nil {
		return fromStorageError(err)
	}
	w.Header().Set("Location", "/"+bucket)
	w.WriteHeader(http.StatusOK)
	return nil
}

func (h *Handler) headBucket(w http.ResponseWriter, r *http.Request, bucket string) error {
	if _, err := h.store.GetBucket(r.Context(), bucket); err != nil {
		return fromStorageError(err)
	}
	w.Header().Set("X-Amz-Bucket-Region", h.region)
	w.WriteHeader(http.StatusOK)
	return nil
}

func (h *Handler) deleteBucket(w http.ResponseWriter, r *http.Request, bucket string) error {
	if err := h.store.DeleteBucket(r.Context(), bucket); err != nil {
		return fromStorageError(err)
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (h *Handler) bucketLocation(w http.ResponseWriter, r *http.Request, bucket string) error {
	if _, err := h.store.GetBucket(r.Context(), bucket); err != nil {
		return fromStorageError(err)
	}
	region := h.region
	if region == "us-east-1" {
		region = ""
	}
	response := struct {
		XMLName xml.Name `xml:"LocationConstraint"`
		XMLNS   string   `xml:"xmlns,attr"`
		Value   string   `xml:",chardata"`
	}{XMLNS: namespace, Value: region}
	writeXML(w, http.StatusOK, response)
	return nil
}

func (h *Handler) putObject(w http.ResponseWriter, r *http.Request, result auth.Result, bucket, key string) error {
	if r.ContentLength < 0 && len(r.TransferEncoding) == 0 {
		return apiError("MissingContentLength", http.StatusLengthRequired, "You must provide the Content-Length HTTP header")
	}
	options := storage.PutOptions{
		Metadata: storage.Metadata{
			ContentType: r.Header.Get("Content-Type"), ContentEncoding: r.Header.Get("Content-Encoding"),
			ContentDisposition: r.Header.Get("Content-Disposition"), ContentLanguage: r.Header.Get("Content-Language"),
			CacheControl: r.Header.Get("Cache-Control"), Expires: r.Header.Get("Expires"), User: userMetadata(r.Header),
		},
		MaxSize: h.maxWrite, SHA256: result.ExpectedSHA256,
		IfMatch: r.Header.Get("If-Match"), IfNoneMatch: r.Header.Get("If-None-Match"),
	}
	obj, err := h.store.PutObject(r.Context(), bucket, key, r.Body, options)
	if err != nil {
		if strings.Contains(err.Error(), "SHA256") {
			return apiError("XAmzContentSHA256Mismatch", http.StatusBadRequest, "payload checksum did not match")
		}
		return fromStorageError(err)
	}
	w.Header().Set("ETag", quoteETag(obj.ETag))
	w.WriteHeader(http.StatusOK)
	return nil
}

func (h *Handler) getObject(w http.ResponseWriter, r *http.Request, bucket, key string, head bool) error {
	obj, file, err := h.store.OpenObject(r.Context(), bucket, key)
	if err != nil {
		return fromStorageError(err)
	}
	defer file.Close()
	if err := checkReadConditions(r, obj); err != nil {
		return err
	}
	start, length, partial, err := resolveRange(r.Header.Get("Range"), obj.Size)
	if err != nil {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", obj.Size))
		return apiError("InvalidRange", http.StatusRequestedRangeNotSatisfiable, "The requested range is not satisfiable")
	}
	setObjectHeaders(w.Header(), obj)
	w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
	if partial {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, start+length-1, obj.Size))
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	if head || length == 0 {
		return nil
	}
	if _, err := file.Seek(start, io.SeekStart); err != nil {
		return internalError(err)
	}
	_, err = io.CopyN(w, file, length)
	return err
}

func (h *Handler) deleteObject(w http.ResponseWriter, r *http.Request, bucket, key string) error {
	if err := h.store.DeleteObject(r.Context(), bucket, key); err != nil {
		return fromStorageError(err)
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (h *Handler) listObjects(w http.ResponseWriter, r *http.Request, bucket string) error {
	query := r.URL.Query()
	limit := 1000
	if raw := query.Get("max-keys"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 0 {
			return apiError("InvalidArgument", http.StatusBadRequest, "max-keys must be a non-negative integer")
		}
		if parsed < limit {
			limit = parsed
		}
	}
	v2 := query.Get("list-type") == "2"
	marker := query.Get("marker")
	if v2 {
		marker = query.Get("start-after")
		if token := query.Get("continuation-token"); token != "" {
			decoded, err := base64.RawURLEncoding.DecodeString(token)
			if err != nil {
				return apiError("InvalidArgument", http.StatusBadRequest, "invalid continuation token")
			}
			marker = string(decoded)
		}
	}
	result, err := h.store.ListObjects(r.Context(), bucket, storage.ListOptions{
		Prefix: query.Get("prefix"), Delimiter: query.Get("delimiter"), Marker: marker, Limit: limit,
	})
	if err != nil {
		return fromStorageError(err)
	}
	type content struct {
		Key          string `xml:"Key"`
		LastModified string `xml:"LastModified"`
		ETag         string `xml:"ETag"`
		Size         int64  `xml:"Size"`
		StorageClass string `xml:"StorageClass"`
	}
	type commonPrefix struct {
		Prefix string `xml:"Prefix"`
	}
	response := struct {
		XMLName               xml.Name       `xml:"ListBucketResult"`
		XMLNS                 string         `xml:"xmlns,attr"`
		Name                  string         `xml:"Name"`
		Prefix                string         `xml:"Prefix"`
		Marker                string         `xml:"Marker,omitempty"`
		MaxKeys               int            `xml:"MaxKeys"`
		KeyCount              *int           `xml:"KeyCount,omitempty"`
		IsTruncated           bool           `xml:"IsTruncated"`
		NextMarker            string         `xml:"NextMarker,omitempty"`
		ContinuationToken     string         `xml:"ContinuationToken,omitempty"`
		NextContinuationToken string         `xml:"NextContinuationToken,omitempty"`
		Contents              []content      `xml:"Contents"`
		CommonPrefixes        []commonPrefix `xml:"CommonPrefixes"`
	}{XMLNS: namespace, Name: bucket, Prefix: query.Get("prefix"), Marker: marker, MaxKeys: limit, IsTruncated: result.Truncated}
	for _, obj := range result.Objects {
		response.Contents = append(response.Contents, content{Key: obj.Key, LastModified: iso8601(obj.LastModified), ETag: quoteETag(obj.ETag), Size: obj.Size, StorageClass: "STANDARD"})
	}
	for _, prefix := range result.Prefixes {
		response.CommonPrefixes = append(response.CommonPrefixes, commonPrefix{Prefix: prefix})
	}
	if v2 {
		count := len(result.Objects) + len(result.Prefixes)
		response.KeyCount = &count
		response.Marker = ""
		response.ContinuationToken = query.Get("continuation-token")
		if result.Truncated {
			response.NextContinuationToken = base64.RawURLEncoding.EncodeToString([]byte(result.NextMarker))
		}
	} else if result.Truncated {
		response.NextMarker = result.NextMarker
	}
	writeXML(w, http.StatusOK, response)
	return nil
}

func (h *Handler) splitPath(r *http.Request) (string, string, error) {
	host := r.Host
	if parsed, _, err := net.SplitHostPort(host); err == nil {
		host = parsed
	}
	if h.domain != "" {
		lower := strings.ToLower(strings.TrimSuffix(host, "."))
		if prefix, ok := strings.CutSuffix(lower, "."+h.domain); ok && prefix != "" {
			key, err := url.PathUnescape(strings.TrimPrefix(r.URL.EscapedPath(), "/"))
			return prefix, key, err
		}
	}
	path := strings.TrimPrefix(r.URL.EscapedPath(), "/")
	bucketRaw, keyRaw, hasKey := strings.Cut(path, "/")
	bucket, err := url.PathUnescape(bucketRaw)
	if err != nil {
		return "", "", err
	}
	if !hasKey {
		return bucket, "", nil
	}
	key, err := url.PathUnescape(keyRaw)
	return bucket, key, err
}

type apiErr struct {
	Code    string
	Status  int
	Message string
}

func (e *apiErr) Error() string { return e.Code + ": " + e.Message }
func apiError(code string, status int, message string) error {
	return &apiErr{Code: code, Status: status, Message: message}
}
func internalError(err error) error {
	return apiError("InternalError", http.StatusInternalServerError, err.Error())
}
func methodNotAllowed() error {
	return apiError("MethodNotAllowed", http.StatusMethodNotAllowed, "The specified method is not allowed")
}

func fromAuthError(err error) error {
	var authErr *auth.Error
	if errors.As(err, &authErr) {
		status := http.StatusForbidden
		if authErr.Code == "AuthorizationHeaderMalformed" || authErr.Code == "AuthorizationQueryParametersError" || authErr.Code == "InvalidArgument" {
			status = http.StatusBadRequest
		}
		return apiError(authErr.Code, status, authErr.Message)
	}
	return internalError(err)
}

func fromStorageError(err error) error {
	switch {
	case errors.Is(err, storage.ErrNoSuchBucket):
		return apiError("NoSuchBucket", http.StatusNotFound, "The specified bucket does not exist")
	case errors.Is(err, storage.ErrBucketExists):
		return apiError("BucketAlreadyOwnedByYou", http.StatusConflict, "The bucket already exists")
	case errors.Is(err, storage.ErrBucketNotEmpty):
		return apiError("BucketNotEmpty", http.StatusConflict, "The bucket is not empty")
	case errors.Is(err, storage.ErrNoSuchKey):
		return apiError("NoSuchKey", http.StatusNotFound, "The specified key does not exist")
	case errors.Is(err, storage.ErrPreconditionFailed):
		return apiError("PreconditionFailed", http.StatusPreconditionFailed, "A precondition did not hold")
	case errors.Is(err, storage.ErrEntityTooLarge):
		return apiError("EntityTooLarge", http.StatusBadRequest, "The object is too large")
	default:
		if strings.Contains(err.Error(), "invalid bucket") {
			return apiError("InvalidBucketName", http.StatusBadRequest, "The specified bucket is not valid")
		}
		return internalError(err)
	}
}

func (h *Handler) writeError(w http.ResponseWriter, r *http.Request, requestID string, err error) {
	api := &apiErr{Code: "InternalError", Status: http.StatusInternalServerError, Message: err.Error()}
	_ = errors.As(err, &api)
	if r.Method == http.MethodHead {
		w.WriteHeader(api.Status)
		return
	}
	response := struct {
		XMLName   xml.Name `xml:"Error"`
		Code      string   `xml:"Code"`
		Message   string   `xml:"Message"`
		Resource  string   `xml:"Resource"`
		RequestID string   `xml:"RequestId"`
	}{Code: api.Code, Message: api.Message, Resource: r.URL.Path, RequestID: requestID}
	writeXML(w, api.Status, response)
}

type owner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

func defaultOwner() owner { return owner{ID: "objex", DisplayName: "objex"} }

func writeXML(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, xml.Header)
	_ = xml.NewEncoder(w).Encode(value)
}

func setObjectHeaders(header http.Header, obj storage.Object) {
	header.Set("ETag", quoteETag(obj.ETag))
	header.Set("Last-Modified", obj.LastModified.UTC().Format(http.TimeFormat))
	header.Set("Accept-Ranges", "bytes")
	header.Set("X-Amz-Storage-Class", "STANDARD")
	if obj.Metadata.ContentType != "" {
		header.Set("Content-Type", obj.Metadata.ContentType)
	} else {
		header.Set("Content-Type", "application/octet-stream")
	}
	if obj.Metadata.ContentEncoding != "" {
		header.Set("Content-Encoding", obj.Metadata.ContentEncoding)
	}
	if obj.Metadata.ContentDisposition != "" {
		header.Set("Content-Disposition", obj.Metadata.ContentDisposition)
	}
	if obj.Metadata.ContentLanguage != "" {
		header.Set("Content-Language", obj.Metadata.ContentLanguage)
	}
	if obj.Metadata.CacheControl != "" {
		header.Set("Cache-Control", obj.Metadata.CacheControl)
	}
	if obj.Metadata.Expires != "" {
		header.Set("Expires", obj.Metadata.Expires)
	}
	for key, value := range obj.Metadata.User {
		header.Set("X-Amz-Meta-"+key, value)
	}
}

func userMetadata(header http.Header) map[string]string {
	metadata := make(map[string]string)
	for key, values := range header {
		if name, ok := strings.CutPrefix(strings.ToLower(key), "x-amz-meta-"); ok {
			metadata[name] = strings.Join(values, ",")
		}
	}
	if len(metadata) == 0 {
		return nil
	}
	return metadata
}

func resolveRange(value string, size int64) (int64, int64, bool, error) {
	if value == "" {
		return 0, size, false, nil
	}
	spec, ok := strings.CutPrefix(strings.TrimSpace(value), "bytes=")
	if !ok || strings.Contains(spec, ",") {
		return 0, size, false, nil
	}
	left, right, ok := strings.Cut(spec, "-")
	if !ok {
		return 0, 0, false, errors.New("invalid range")
	}
	if left == "" {
		suffix, err := strconv.ParseInt(right, 10, 64)
		if err != nil || suffix <= 0 || size == 0 {
			return 0, 0, false, errors.New("invalid range")
		}
		if suffix > size {
			suffix = size
		}
		return size - suffix, suffix, true, nil
	}
	start, err := strconv.ParseInt(left, 10, 64)
	if err != nil || start < 0 || start >= size {
		return 0, 0, false, errors.New("invalid range")
	}
	end := size - 1
	if right != "" {
		end, err = strconv.ParseInt(right, 10, 64)
		if err != nil || end < start {
			return 0, 0, false, errors.New("invalid range")
		}
		if end >= size {
			end = size - 1
		}
	}
	return start, end - start + 1, true, nil
}

func checkReadConditions(r *http.Request, obj storage.Object) error {
	etag := quoteETag(obj.ETag)
	if value := r.Header.Get("If-Match"); value != "" && value != "*" && !strings.Contains(value, etag) {
		return apiError("PreconditionFailed", http.StatusPreconditionFailed, "A precondition did not hold")
	}
	if value := r.Header.Get("If-None-Match"); value == "*" || (value != "" && strings.Contains(value, etag)) {
		return apiError("NotModified", http.StatusNotModified, "Not Modified")
	}
	return nil
}

func quoteETag(value string) string  { return "\"" + value + "\"" }
func iso8601(value time.Time) string { return value.UTC().Format("2006-01-02T15:04:05.000Z") }

func randomRequestID() string {
	var raw [8]byte
	_, _ = rand.Read(raw[:])
	return strings.ToUpper(hex.EncodeToString(raw[:]))
}
