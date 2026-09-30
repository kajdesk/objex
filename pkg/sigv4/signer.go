// Package sigv4 signs HTTP requests using AWS Signature Version 4.
// It is intentionally endpoint-agnostic so the benchmark can target either
// objex with the same authentication path used by external S3 clients.
package sigv4

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

const Algorithm = "AWS4-HMAC-SHA256"

type Credentials struct {
	AccessKey string
	SecretKey string
}

type Signer struct {
	Credentials Credentials
	Region      string
	Service     string
	Now         func() time.Time
}

func (s Signer) Sign(req *http.Request, payloadHash string) error {
	if s.Credentials.AccessKey == "" || s.Credentials.SecretKey == "" {
		return fmt.Errorf("sigv4: credentials are required")
	}
	if s.Region == "" {
		s.Region = "auto"
	}
	if s.Service == "" {
		s.Service = "s3"
	}
	now := time.Now().UTC()
	if s.Now != nil {
		now = s.Now().UTC()
	}
	if payloadHash == "" {
		payloadHash = hexSHA256(nil)
	}
	amzDate := now.Format("20060102T150405Z")
	date := now.Format("20060102")
	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	if req.Host == "" {
		req.Host = req.URL.Host
	}
	signedHeaders := []string{"host", "x-amz-content-sha256", "x-amz-date"}
	canonicalHeaders := "host:" + normalize(req.Host) + "\n" +
		"x-amz-content-sha256:" + payloadHash + "\n" +
		"x-amz-date:" + amzDate + "\n"
	canonicalRequest := strings.Join([]string{
		req.Method,
		CanonicalURI(req.URL),
		CanonicalQuery(req.URL.Query(), ""),
		canonicalHeaders,
		strings.Join(signedHeaders, ";"),
		payloadHash,
	}, "\n")
	scope := strings.Join([]string{date, s.Region, s.Service, "aws4_request"}, "/")
	stringToSign := strings.Join([]string{Algorithm, amzDate, scope, hexSHA256([]byte(canonicalRequest))}, "\n")
	signature := hex.EncodeToString(hmacSHA256(signingKey(s.Credentials.SecretKey, date, s.Region, s.Service), []byte(stringToSign)))
	req.Header.Set("Authorization", fmt.Sprintf("%s Credential=%s/%s,SignedHeaders=%s,Signature=%s", Algorithm, s.Credentials.AccessKey, scope, strings.Join(signedHeaders, ";"), signature))
	return nil
}

// CanonicalQuery returns the SigV4 encoded and sorted query string. skip omits
// a parameter such as X-Amz-Signature during presigned request verification.
func CanonicalQuery(values url.Values, skip string) string {
	pairs := make([]string, 0, len(values))
	for key, all := range values {
		if key == skip {
			continue
		}
		if len(all) == 0 {
			all = []string{""}
		}
		for _, value := range all {
			pairs = append(pairs, awsEncode(key)+"="+awsEncode(value))
		}
	}
	sort.Strings(pairs)
	return strings.Join(pairs, "&")
}

func CanonicalURI(u *url.URL) string {
	path := u.EscapedPath()
	if path == "" {
		return "/"
	}
	// EscapedPath uses uppercase hex and preserves '/', which matches S3 SigV4.
	return path
}

func awsEncode(value string) string {
	return strings.ReplaceAll(url.QueryEscape(value), "+", "%20")
}

func normalize(value string) string { return strings.Join(strings.Fields(value), " ") }

func signingKey(secret, date, region, service string) []byte {
	dateKey := hmacSHA256([]byte("AWS4"+secret), []byte(date))
	regionKey := hmacSHA256(dateKey, []byte(region))
	serviceKey := hmacSHA256(regionKey, []byte(service))
	return hmacSHA256(serviceKey, []byte("aws4_request"))
}

func hmacSHA256(key, data []byte) []byte {
	hash := hmac.New(sha256.New, key)
	_, _ = hash.Write(data)
	return hash.Sum(nil)
}

func hexSHA256(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
