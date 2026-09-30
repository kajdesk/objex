package s3

import (
	"encoding/base64"
	"encoding/xml"
	"net/http"
	"strings"

	"github.com/kajdesk/objex/internal/auth"
	"github.com/kajdesk/objex/internal/s3err"
	"github.com/kajdesk/objex/internal/storage"
)

func (h *Handler) listBuckets(q *request) error {
	buckets, err := h.store.ListBuckets(q.r.Context())
	if err != nil {
		return err
	}
	type bucket struct {
		Name         string
		CreationDate string
	}
	out := struct {
		XMLName xml.Name `xml:"ListAllMyBucketsResult"`
		XMLNS   string   `xml:"xmlns,attr"`
		Owner   owner
		Buckets []bucket `xml:"Buckets>Bucket"`
	}{XMLNS: xmlns, Owner: theOwner}
	for _, b := range buckets {
		if q.auth.Key.CanAccess(b.Name) {
			out.Buckets = append(out.Buckets, bucket{b.Name, iso8601(b.Created)})
		}
	}
	writeXML(q.w, http.StatusOK, out)
	return nil
}

// cannedACL parses x-amz-acl into "public read?"; ok is false when absent.
func cannedACL(v string) (public, ok bool, err error) {
	switch strings.TrimSpace(v) {
	case "":
		return false, false, nil
	case "private", "bucket-owner-full-control", "bucket-owner-read":
		return false, true, nil
	case "public-read", "public-read-write":
		return true, true, nil
	}
	return false, false, s3err.InvalidArgument.WithMessage("Unsupported canned ACL " + v)
}

func (h *Handler) createBucket(q *request) error {
	public, _, err := cannedACL(q.header("X-Amz-Acl"))
	if err != nil {
		return err
	}
	// Any CreateBucketConfiguration (location constraint) is accepted and ignored.
	if _, err := q.readBody(); err != nil {
		return err
	}
	if err := h.store.CreateBucket(q.r.Context(), q.bucket, public); err != nil {
		return err
	}
	q.w.Header().Set("Location", "/"+q.bucket)
	return ok(q.w, http.StatusOK)
}

func (h *Handler) headBucket(q *request) error {
	if _, err := h.store.GetBucket(q.r.Context(), q.bucket); err != nil {
		return err
	}
	q.w.Header().Set("X-Amz-Bucket-Region", h.region)
	return ok(q.w, http.StatusOK)
}

func (h *Handler) deleteBucket(q *request) error {
	if err := h.store.DeleteBucket(q.r.Context(), q.bucket); err != nil {
		return err
	}
	return ok(q.w, http.StatusNoContent)
}

func (h *Handler) bucketLocation(q *request) error {
	if _, err := h.store.GetBucket(q.r.Context(), q.bucket); err != nil {
		return err
	}
	region := h.region
	if region == "us-east-1" {
		region = ""
	}
	writeXML(q.w, http.StatusOK, struct {
		XMLName xml.Name `xml:"LocationConstraint"`
		XMLNS   string   `xml:"xmlns,attr"`
		Value   string   `xml:",chardata"`
	}{XMLNS: xmlns, Value: region})
	return nil
}

func (h *Handler) versioning(q *request) error {
	if _, err := h.store.GetBucket(q.r.Context(), q.bucket); err != nil {
		return err
	}
	writeXML(q.w, http.StatusOK, struct {
		XMLName xml.Name `xml:"VersioningConfiguration"`
		XMLNS   string   `xml:"xmlns,attr"`
	}{XMLNS: xmlns})
	return nil
}

// ---------------------------------------------------------------------------
// ACLs (a bucket is private or public-read; object ACLs follow the bucket)
// ---------------------------------------------------------------------------

type grantee struct {
	XMLNSXSI    string `xml:"xmlns:xsi,attr"`
	Type        string `xml:"xsi:type,attr"`
	ID          string `xml:"ID,omitempty"`
	DisplayName string `xml:"DisplayName,omitempty"`
	URI         string `xml:"URI,omitempty"`
}

type grant struct {
	Grantee    grantee
	Permission string
}

func aclXML(public bool) any {
	const xsi = "http://www.w3.org/2001/XMLSchema-instance"
	grants := []grant{{Grantee: grantee{XMLNSXSI: xsi, Type: "CanonicalUser", ID: theOwner.ID, DisplayName: theOwner.DisplayName}, Permission: "FULL_CONTROL"}}
	if public {
		grants = append(grants, grant{Grantee: grantee{XMLNSXSI: xsi, Type: "Group", URI: allUsersURI}, Permission: "READ"})
	}
	return struct {
		XMLName xml.Name `xml:"AccessControlPolicy"`
		XMLNS   string   `xml:"xmlns,attr"`
		Owner   owner
		Grants  []grant `xml:"AccessControlList>Grant"`
	}{XMLNS: xmlns, Owner: theOwner, Grants: grants}
}

func (h *Handler) getBucketACL(q *request) error {
	b, err := h.store.GetBucket(q.r.Context(), q.bucket)
	if err != nil {
		return err
	}
	writeXML(q.w, http.StatusOK, aclXML(b.PublicRead))
	return nil
}

func (h *Handler) putBucketACL(q *request) error {
	public, set, err := cannedACL(q.header("X-Amz-Acl"))
	if err != nil {
		return err
	}
	if !set {
		// An AccessControlPolicy body: public if it grants AllUsers READ.
		var policy struct {
			Grants []struct {
				URI        string `xml:"Grantee>URI"`
				Permission string
			} `xml:"AccessControlList>Grant"`
		}
		if err := q.readXML(&policy); err != nil {
			return err
		}
		for _, g := range policy.Grants {
			if g.URI == allUsersURI && (g.Permission == "READ" || g.Permission == "FULL_CONTROL") {
				public = true
			}
		}
	}
	if err := h.store.UpdateBucket(q.r.Context(), q.bucket, func(b *storage.Bucket) { b.PublicRead = public }); err != nil {
		return err
	}
	return ok(q.w, http.StatusOK)
}

// ---------------------------------------------------------------------------
// CORS configuration
// ---------------------------------------------------------------------------

type corsRuleXML struct {
	ID             string   `xml:"ID,omitempty"`
	AllowedOrigins []string `xml:"AllowedOrigin"`
	AllowedMethods []string `xml:"AllowedMethod"`
	AllowedHeaders []string `xml:"AllowedHeader,omitempty"`
	ExposeHeaders  []string `xml:"ExposeHeader,omitempty"`
	MaxAgeSeconds  int      `xml:"MaxAgeSeconds,omitempty"`
}

type corsXML struct {
	XMLName xml.Name      `xml:"CORSConfiguration"`
	XMLNS   string        `xml:"xmlns,attr,omitempty"`
	Rules   []corsRuleXML `xml:"CORSRule"`
}

func (h *Handler) getCORS(q *request) error {
	b, err := h.store.GetBucket(q.r.Context(), q.bucket)
	if err != nil {
		return err
	}
	if len(b.CORS) == 0 {
		return s3err.NoSuchCORSConfiguration
	}
	out := corsXML{XMLNS: xmlns}
	for _, r := range b.CORS {
		out.Rules = append(out.Rules, corsRuleXML(r))
	}
	writeXML(q.w, http.StatusOK, out)
	return nil
}

func (h *Handler) putCORS(q *request) error {
	var in corsXML
	if err := q.readXML(&in); err != nil {
		return err
	}
	if len(in.Rules) == 0 || len(in.Rules) > 100 {
		return s3err.MalformedXML.WithMessage("A CORS configuration must have between 1 and 100 rules")
	}
	rules := make([]storage.CORSRule, 0, len(in.Rules))
	for _, r := range in.Rules {
		if len(r.AllowedOrigins) == 0 || len(r.AllowedMethods) == 0 {
			return s3err.MalformedXML.WithMessage("Each CORS rule needs an AllowedOrigin and an AllowedMethod")
		}
		for _, m := range r.AllowedMethods {
			switch m {
			case "GET", "PUT", "HEAD", "POST", "DELETE":
			default:
				return s3err.InvalidRequest.WithMessage("Found unsupported HTTP method in CORS config. Unsupported method is " + m)
			}
		}
		rules = append(rules, storage.CORSRule(r))
	}
	if err := h.store.UpdateBucket(q.r.Context(), q.bucket, func(b *storage.Bucket) { b.CORS = rules }); err != nil {
		return err
	}
	return ok(q.w, http.StatusOK)
}

func (h *Handler) deleteCORS(q *request) error {
	if err := h.store.UpdateBucket(q.r.Context(), q.bucket, func(b *storage.Bucket) { b.CORS = nil }); err != nil {
		return err
	}
	return ok(q.w, http.StatusNoContent)
}

// ---------------------------------------------------------------------------
// DeleteObjects and listing
// ---------------------------------------------------------------------------

func (h *Handler) deleteObjects(q *request) error {
	var in struct {
		Quiet   bool
		Objects []struct{ Key string } `xml:"Object"`
	}
	if err := q.readXML(&in); err != nil {
		return err
	}
	if len(in.Objects) > 1000 {
		return s3err.MalformedXML.WithMessage("The request must contain no more than 1000 keys")
	}
	keys := make([]string, len(in.Objects))
	for i, o := range in.Objects {
		keys[i] = o.Key
	}
	results, err := h.store.DeleteObjects(q.r.Context(), q.bucket, keys)
	if err != nil {
		return err
	}
	type deleted struct{ Key string }
	type failed struct{ Key, Code, Message string }
	out := struct {
		XMLName xml.Name  `xml:"DeleteResult"`
		XMLNS   string    `xml:"xmlns,attr"`
		Deleted []deleted `xml:"Deleted"`
		Errors  []failed  `xml:"Error"`
	}{XMLNS: xmlns}
	for i, k := range keys {
		if results[i] != nil {
			e := s3err.As(results[i])
			out.Errors = append(out.Errors, failed{k, e.Code, e.Message})
		} else if !in.Quiet {
			out.Deleted = append(out.Deleted, deleted{k})
		}
	}
	writeXML(q.w, http.StatusOK, out)
	return nil
}

func (h *Handler) listObjects(q *request) error {
	v2 := q.q("list-type") == "2"
	urlEncode := false
	switch q.q("encoding-type") {
	case "":
	case "url":
		urlEncode = true
	default:
		return s3err.InvalidArgument.WithMessage("Invalid Encoding Method specified in Request")
	}
	enc := func(s string) string {
		if urlEncode {
			return auth.EncodePath(s)
		}
		return s
	}
	limit, err := parseMaxKeys(q.q("max-keys"), "max-keys")
	if err != nil {
		return err
	}
	prefix, delim, startAfter := q.q("prefix"), q.q("delimiter"), q.q("start-after")
	token := q.q("continuation-token")
	marker := q.q("marker")
	if v2 {
		marker = startAfter
		if q.has("continuation-token") {
			raw, err := base64.RawURLEncoding.DecodeString(token)
			if err != nil {
				return s3err.InvalidArgument.WithMessage("The continuation token provided is incorrect")
			}
			marker = string(raw)
		}
	}
	res, err := h.store.ListObjects(q.r.Context(), q.bucket, storage.ListOptions{Prefix: prefix, Delimiter: delim, Marker: marker, Limit: limit})
	if err != nil {
		return err
	}
	type content struct {
		Key               string
		LastModified      string
		ETag              string
		Size              int64
		ChecksumAlgorithm string `xml:",omitempty"`
		ChecksumType      string `xml:",omitempty"`
		StorageClass      string
		Owner             *owner `xml:",omitempty"`
	}
	type commonPrefix struct{ Prefix string }
	out := struct {
		XMLName               xml.Name `xml:"ListBucketResult"`
		XMLNS                 string   `xml:"xmlns,attr"`
		Name                  string
		Prefix                string
		Marker                *string `xml:",omitempty"`
		StartAfter            string  `xml:",omitempty"`
		ContinuationToken     string  `xml:",omitempty"`
		KeyCount              *int    `xml:",omitempty"`
		MaxKeys               int
		Delimiter             string `xml:",omitempty"`
		IsTruncated           bool
		NextMarker            string         `xml:",omitempty"`
		NextContinuationToken string         `xml:",omitempty"`
		EncodingType          string         `xml:",omitempty"`
		Contents              []content      `xml:"Contents"`
		CommonPrefixes        []commonPrefix `xml:"CommonPrefixes"`
	}{XMLNS: xmlns, Name: q.bucket, Prefix: enc(prefix), MaxKeys: limit, Delimiter: enc(delim), IsTruncated: res.Truncated}
	if urlEncode {
		out.EncodingType = "url"
	}
	fetchOwner := !v2 || q.q("fetch-owner") == "true"
	for _, o := range res.Objects {
		c := content{Key: enc(o.Key), LastModified: iso8601(o.LastModified), ETag: quote(o.ETag), Size: o.Size, StorageClass: "STANDARD"}
		if o.Checksum != nil {
			c.ChecksumAlgorithm, c.ChecksumType = string(o.Checksum.Algo), string(o.Checksum.Type())
		}
		if fetchOwner {
			c.Owner = &theOwner
		}
		out.Contents = append(out.Contents, c)
	}
	for _, p := range res.Prefixes {
		out.CommonPrefixes = append(out.CommonPrefixes, commonPrefix{enc(p)})
	}
	if v2 {
		n := len(res.Objects) + len(res.Prefixes)
		out.KeyCount, out.StartAfter, out.ContinuationToken = &n, enc(startAfter), token
		if res.Truncated {
			out.NextContinuationToken = base64.RawURLEncoding.EncodeToString([]byte(res.NextMarker))
		}
	} else {
		m := enc(marker)
		out.Marker = &m
		if res.Truncated {
			out.NextMarker = enc(res.NextMarker)
		}
	}
	writeXML(q.w, http.StatusOK, out)
	return nil
}
