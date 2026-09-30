package s3

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/kajdesk/objex/internal/s3err"
	"github.com/kajdesk/objex/internal/storage"
)

// wildcard matches with at most one '*', as S3 allows in origins and headers.
func wildcard(pattern, value string, fold bool) bool {
	if fold {
		pattern, value = strings.ToLower(pattern), strings.ToLower(value)
	}
	pre, suf, found := strings.Cut(pattern, "*")
	if !found {
		return pattern == value
	}
	return len(value) >= len(pre)+len(suf) && strings.HasPrefix(value, pre) && strings.HasSuffix(value, suf)
}

func findRule(rules []storage.CORSRule, origin, method string, headers []string) *storage.CORSRule {
	for i := range rules {
		r := &rules[i]
		if !anyMatch(r.AllowedOrigins, origin, false) || !contains(r.AllowedMethods, method) {
			continue
		}
		allowed := true
		for _, h := range headers {
			if !anyMatch(r.AllowedHeaders, h, true) {
				allowed = false
				break
			}
		}
		if allowed {
			return r
		}
	}
	return nil
}

func anyMatch(patterns []string, v string, fold bool) bool {
	for _, p := range patterns {
		if wildcard(p, v, fold) {
			return true
		}
	}
	return false
}

func contains(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

func setCORSHeaders(r *storage.CORSRule, origin string, h http.Header) {
	allow := origin
	if contains(r.AllowedOrigins, "*") {
		allow = "*"
	} else {
		h.Set("Access-Control-Allow-Credentials", "true")
	}
	h.Set("Access-Control-Allow-Origin", allow)
	h.Set("Access-Control-Allow-Methods", strings.Join(r.AllowedMethods, ", "))
	if len(r.ExposeHeaders) > 0 {
		h.Set("Access-Control-Expose-Headers", strings.Join(r.ExposeHeaders, ", "))
	}
	if r.MaxAgeSeconds > 0 {
		h.Set("Access-Control-Max-Age", strconv.Itoa(r.MaxAgeSeconds))
	}
	h.Add("Vary", "Origin, Access-Control-Request-Headers, Access-Control-Request-Method")
}

// applyCORS adds CORS headers to a normal response when a rule matches.
func applyCORS(rules []storage.CORSRule, origin, method string, h http.Header) {
	if r := findRule(rules, origin, method, nil); r != nil {
		setCORSHeaders(r, origin, h)
	}
}

// preflight answers an OPTIONS request. No authentication is involved.
func (h *Handler) preflight(q *request) error {
	origin, method := q.header("Origin"), q.header("Access-Control-Request-Method")
	if origin == "" || method == "" {
		return s3err.InvalidRequest.WithMessage("Insufficient information. Origin request header needed.")
	}
	if q.bucket == "" {
		return s3err.CORSForbidden
	}
	b, err := h.store.GetBucket(q.r.Context(), q.bucket)
	if err != nil {
		return err
	}
	var requested []string
	for _, v := range strings.Split(q.header("Access-Control-Request-Headers"), ",") {
		if v = strings.ToLower(strings.TrimSpace(v)); v != "" {
			requested = append(requested, v)
		}
	}
	rule := findRule(b.CORS, origin, method, requested)
	if rule == nil {
		return s3err.CORSForbidden
	}
	setCORSHeaders(rule, origin, q.w.Header())
	if len(requested) > 0 {
		q.w.Header().Set("Access-Control-Allow-Headers", strings.Join(requested, ", "))
	}
	return ok(q.w, http.StatusOK)
}
