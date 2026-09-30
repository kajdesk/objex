package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strings"
	"time"

	"github.com/kajdesk/objex/internal/config"
	"github.com/kajdesk/objex/pkg/sigv4"
)

const (
	UnsignedPayload = "UNSIGNED-PAYLOAD"
	maxSkew         = 15 * time.Minute
	maxPresign      = 7 * 24 * time.Hour
)

type Error struct {
	Code    string
	Message string
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

type Result struct {
	Key            *config.Key
	ExpectedSHA256 []byte
}

type Verifier struct {
	keys map[string]config.Key
}

func New(keys []config.Key) *Verifier {
	indexed := make(map[string]config.Key, len(keys))
	for _, key := range keys {
		indexed[key.AccessKey] = key
	}
	return &Verifier{keys: indexed}
}

func (v *Verifier) Authenticate(req *http.Request, now time.Time) (Result, error) {
	if authorization := req.Header.Get("Authorization"); authorization != "" {
		return v.header(req, authorization, now.UTC())
	}
	if req.URL.Query().Get("X-Amz-Algorithm") != "" {
		return v.presigned(req, now.UTC())
	}
	return Result{}, nil
}

func (v *Verifier) header(req *http.Request, authorization string, now time.Time) (Result, error) {
	rest, ok := strings.CutPrefix(authorization, sigv4.Algorithm+" ")
	if !ok {
		return Result{}, authError("InvalidRequest", "unsupported authorization type")
	}
	fields := parseFields(rest)
	credential, headers, signature := fields["Credential"], fields["SignedHeaders"], fields["Signature"]
	if credential == "" || headers == "" || signature == "" {
		return Result{}, authError("AuthorizationHeaderMalformed", "authorization header is incomplete")
	}
	amzDate := req.Header.Get("X-Amz-Date")
	requestTime, err := time.Parse("20060102T150405Z", amzDate)
	if err != nil {
		return Result{}, authError("AccessDenied", "invalid x-amz-date")
	}
	if durationAbs(now.Sub(requestTime)) >= maxSkew {
		return Result{}, authError("RequestTimeTooSkewed", "request time differs too much from server time")
	}
	payloadHash := req.Header.Get("X-Amz-Content-Sha256")
	if payloadHash == "" {
		payloadHash = UnsignedPayload
	}
	return v.verify(req, credential, strings.Split(headers, ";"), signature, amzDate, payloadHash, false)
}

func (v *Verifier) presigned(req *http.Request, now time.Time) (Result, error) {
	query := req.URL.Query()
	if query.Get("X-Amz-Algorithm") != sigv4.Algorithm {
		return Result{}, authError("AuthorizationQueryParametersError", "unsupported signing algorithm")
	}
	credential := query.Get("X-Amz-Credential")
	amzDate := query.Get("X-Amz-Date")
	headers := query.Get("X-Amz-SignedHeaders")
	signature := query.Get("X-Amz-Signature")
	expiresText := query.Get("X-Amz-Expires")
	if credential == "" || amzDate == "" || headers == "" || signature == "" || expiresText == "" {
		return Result{}, authError("AuthorizationQueryParametersError", "presigned request parameters are incomplete")
	}
	requestTime, err := time.Parse("20060102T150405Z", amzDate)
	if err != nil {
		return Result{}, authError("AuthorizationQueryParametersError", "invalid X-Amz-Date")
	}
	expires, err := time.ParseDuration(expiresText + "s")
	if err != nil || expires < 0 || expires > maxPresign {
		return Result{}, authError("AuthorizationQueryParametersError", "invalid X-Amz-Expires")
	}
	if now.Before(requestTime.Add(-maxSkew)) || now.After(requestTime.Add(expires)) {
		return Result{}, authError("AccessDenied", "presigned request is not currently valid")
	}
	payloadHash := query.Get("X-Amz-Content-Sha256")
	if payloadHash == "" {
		payloadHash = UnsignedPayload
	}
	return v.verify(req, credential, strings.Split(headers, ";"), signature, amzDate, payloadHash, true)
}

func (v *Verifier) verify(req *http.Request, credential string, signedHeaders []string, signature, amzDate, payloadHash string, presigned bool) (Result, error) {
	parts := strings.Split(credential, "/")
	if len(parts) != 5 || parts[4] != "aws4_request" || parts[3] != "s3" || !strings.HasPrefix(amzDate, parts[1]) {
		return Result{}, authError("AuthorizationHeaderMalformed", "credential scope is invalid")
	}
	key, ok := v.keys[parts[0]]
	if !ok {
		return Result{}, authError("InvalidAccessKeyId", "access key does not exist")
	}
	canonicalHeaders, err := canonicalHeaders(req, signedHeaders)
	if err != nil {
		return Result{}, err
	}
	if !contains(signedHeaders, "host") {
		return Result{}, authError("AccessDenied", "host must be signed")
	}
	skip := ""
	if presigned {
		skip = "X-Amz-Signature"
	}
	canonicalRequest := strings.Join([]string{
		req.Method,
		sigv4.CanonicalURI(req.URL),
		sigv4.CanonicalQuery(req.URL.Query(), skip),
		canonicalHeaders,
		strings.Join(signedHeaders, ";"),
		payloadHash,
	}, "\n")
	scope := strings.Join(parts[1:], "/")
	requestHash := sha256.Sum256([]byte(canonicalRequest))
	stringToSign := strings.Join([]string{sigv4.Algorithm, amzDate, scope, hex.EncodeToString(requestHash[:])}, "\n")
	expected := hex.EncodeToString(hmacSHA256(signingKey(key.SecretKey, parts[1], parts[2], parts[3]), []byte(stringToSign)))
	if !hmac.Equal([]byte(expected), []byte(signature)) {
		return Result{}, authError("SignatureDoesNotMatch", "request signature does not match")
	}
	result := Result{Key: &key}
	if payloadHash != UnsignedPayload {
		digest, err := hex.DecodeString(payloadHash)
		if err != nil || len(digest) != sha256.Size {
			return Result{}, authError("InvalidArgument", "invalid payload SHA256")
		}
		result.ExpectedSHA256 = digest
	}
	return result, nil
}

func canonicalHeaders(req *http.Request, names []string) (string, error) {
	var builder strings.Builder
	for _, name := range names {
		if name != strings.ToLower(name) {
			return "", authError("AuthorizationHeaderMalformed", "signed headers must be lowercase")
		}
		value := ""
		if name == "host" {
			value = req.Host
			if value == "" {
				value = req.URL.Host
			}
		} else {
			values, ok := req.Header[http.CanonicalHeaderKey(name)]
			if !ok {
				return "", authError("AccessDenied", "signed header is missing: "+name)
			}
			value = strings.Join(values, ",")
		}
		builder.WriteString(name)
		builder.WriteByte(':')
		builder.WriteString(strings.Join(strings.Fields(value), " "))
		builder.WriteByte('\n')
	}
	return builder.String(), nil
}

func parseFields(value string) map[string]string {
	out := make(map[string]string)
	for _, field := range strings.Split(value, ",") {
		key, val, ok := strings.Cut(strings.TrimSpace(field), "=")
		if ok {
			out[key] = val
		}
	}
	return out
}

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

func authError(code, message string) error { return &Error{Code: code, Message: message} }

func durationAbs(value time.Duration) time.Duration {
	if value < 0 {
		return -value
	}
	return value
}

func contains(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}
