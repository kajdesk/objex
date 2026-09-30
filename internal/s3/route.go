package s3

import (
	"net/http"
	"net/url"
	"strings"

	"github.com/kajdesk/objex/internal/s3err"
)

type operation struct {
	name string
	run  func(*Handler, *request) error
	// publicRead operations are allowed anonymously on public-read buckets.
	publicRead bool
	// srcBucket is the copy source bucket, which the key must also access.
	srcBucket, srcKey string
}

func op(name string, run func(*Handler, *request) error) operation {
	return operation{name: name, run: run}
}

// absent reports a configuration objex does not support as "not configured".
func absent(code *s3err.Error) operation {
	return op("absent", func(h *Handler, q *request) error {
		if _, err := h.store.GetBucket(q.r.Context(), q.bucket); err != nil {
			return err
		}
		return code
	})
}

var (
	noContent = op("noContent", func(h *Handler, q *request) error {
		if q.key == "" {
			if _, err := h.store.GetBucket(q.r.Context(), q.bucket); err != nil {
				return err
			}
		}
		return ok(q.w, http.StatusNoContent)
	})
	unsupportedBucketGet = []string{"website", "logging", "notification", "replication", "object-lock", "ownershipControls",
		"requestPayment", "accelerate", "analytics", "inventory", "metrics", "intelligent-tiering", "publicAccessBlock", "policyStatus", "versions"}
)

// parseCopySource parses x-amz-copy-source: "/bucket/key" or "bucket/key",
// URL-encoded, with an optional ?versionId=.
func parseCopySource(v string) (string, string, error) {
	invalid := s3err.InvalidArgument.WithMessage("Copy Source must mention the source bucket and key: sourcebucket/sourcekey")
	v, _, _ = strings.Cut(v, "?versionId=")
	d, err := url.PathUnescape(v)
	if err != nil {
		return "", "", invalid
	}
	b, k, found := strings.Cut(strings.TrimPrefix(d, "/"), "/")
	if !found || b == "" || k == "" {
		return "", "", invalid
	}
	return b, k, nil
}

func route(q *request) (operation, error) {
	m := q.r.Method
	notImpl := s3err.NotImplemented
	if q.bucket == "" {
		if m == http.MethodGet {
			return op("ListBuckets", (*Handler).listBuckets), nil
		}
		return operation{}, s3err.MethodNotAllowed
	}
	if q.key == "" {
		switch m {
		case http.MethodGet:
			switch {
			case q.has("location"):
				return op("GetBucketLocation", (*Handler).bucketLocation), nil
			case q.has("acl"):
				return op("GetBucketAcl", (*Handler).getBucketACL), nil
			case q.has("cors"):
				return op("GetBucketCors", (*Handler).getCORS), nil
			case q.has("versioning"):
				return op("GetBucketVersioning", (*Handler).versioning), nil
			case q.has("uploads"):
				return op("ListMultipartUploads", (*Handler).listUploads), nil
			case q.has("policy"):
				return absent(s3err.NoSuchBucketPolicy), nil
			case q.has("lifecycle"):
				return absent(s3err.NoSuchLifecycleConfiguration), nil
			case q.has("tagging"):
				return absent(s3err.NoSuchTagSet), nil
			case q.has("encryption"):
				return absent(s3err.SSEConfigurationNotFound), nil
			}
			for _, n := range unsupportedBucketGet {
				if q.has(n) {
					return operation{}, notImpl
				}
			}
			o := op("ListObjects", (*Handler).listObjects)
			o.publicRead = true
			return o, nil
		case http.MethodHead:
			return op("HeadBucket", (*Handler).headBucket), nil
		case http.MethodPut:
			switch {
			case q.has("acl"):
				return op("PutBucketAcl", (*Handler).putBucketACL), nil
			case q.has("cors"):
				return op("PutBucketCors", (*Handler).putCORS), nil
			case len(q.query) == 0:
				return op("CreateBucket", (*Handler).createBucket), nil
			}
			return operation{}, notImpl
		case http.MethodDelete:
			switch {
			case q.has("cors"):
				return op("DeleteBucketCors", (*Handler).deleteCORS), nil
			case q.has("policy"), q.has("lifecycle"), q.has("tagging"), q.has("encryption"):
				return noContent, nil
			case len(q.query) == 0:
				return op("DeleteBucket", (*Handler).deleteBucket), nil
			}
			return operation{}, notImpl
		case http.MethodPost:
			if q.has("delete") {
				return op("DeleteObjects", (*Handler).deleteObjects), nil
			}
			return operation{}, notImpl
		}
		return operation{}, s3err.MethodNotAllowed
	}

	var srcBucket, srcKey string
	if v := q.header("X-Amz-Copy-Source"); v != "" {
		var err error
		if srcBucket, srcKey, err = parseCopySource(v); err != nil {
			return operation{}, err
		}
	}
	withSource := func(o operation) operation {
		o.srcBucket, o.srcKey = srcBucket, srcKey
		return o
	}
	switch m {
	case http.MethodGet, http.MethodHead:
		switch {
		case m == http.MethodGet && q.has("uploadId"):
			return op("ListParts", (*Handler).listParts), nil
		case m == http.MethodGet && q.has("acl"):
			return op("GetObjectAcl", (*Handler).getObjectACL), nil
		case m == http.MethodGet && q.has("tagging"):
			return op("GetObjectTagging", (*Handler).getTagging), nil
		case q.has("attributes"), q.has("retention"), q.has("legal-hold"), q.has("torrent"):
			return operation{}, notImpl
		}
		o := op("GetObject", (*Handler).getObject)
		o.publicRead = true
		return o, nil
	case http.MethodPut:
		switch {
		case q.has("uploadId") || q.has("partNumber"):
			if srcBucket != "" {
				return withSource(op("UploadPartCopy", (*Handler).uploadPartCopy)), nil
			}
			return op("UploadPart", (*Handler).uploadPart), nil
		case q.has("acl"):
			return op("PutObjectAcl", (*Handler).putObjectACL), nil
		case q.has("tagging"), q.has("retention"), q.has("legal-hold"):
			return operation{}, notImpl
		case srcBucket != "":
			return withSource(op("CopyObject", (*Handler).copyObject)), nil
		}
		return op("PutObject", (*Handler).putObject), nil
	case http.MethodDelete:
		switch {
		case q.has("uploadId"):
			return op("AbortMultipartUpload", (*Handler).abortUpload), nil
		case q.has("tagging"):
			return noContent, nil
		}
		return op("DeleteObject", (*Handler).deleteObject), nil
	case http.MethodPost:
		switch {
		case q.has("uploads"):
			return op("CreateMultipartUpload", (*Handler).createUpload), nil
		case q.has("uploadId"):
			return op("CompleteMultipartUpload", (*Handler).completeUpload), nil
		}
		return operation{}, notImpl
	}
	return operation{}, s3err.MethodNotAllowed
}
