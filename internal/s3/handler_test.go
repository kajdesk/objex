package s3

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/kajdesk/objex/internal/config"
	"github.com/kajdesk/objex/internal/storage/local"
	"github.com/kajdesk/objex/pkg/sigv4"
)

func TestSignedS3Lifecycle(t *testing.T) {
	t.Parallel()
	store, err := local.Open(t.TempDir(), false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	cfg := config.Default()
	cfg.Keys = []config.Key{{Name: "test", AccessKey: "OBXTEST", SecretKey: "test-secret"}}
	server := httptest.NewServer(New(store, cfg))
	t.Cleanup(server.Close)
	signer := sigv4.Signer{Credentials: sigv4.Credentials{AccessKey: "OBXTEST", SecretKey: "test-secret"}, Region: "auto", Service: "s3"}

	request := func(method, path string, body []byte, headers map[string]string) *http.Response {
		t.Helper()
		req, err := http.NewRequest(method, server.URL+path, bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		for key, value := range headers {
			req.Header.Set(key, value)
		}
		digest := sha256.Sum256(body)
		if err := signer.Sign(req, hex.EncodeToString(digest[:])); err != nil {
			t.Fatal(err)
		}
		response, err := server.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return response
	}

	response := request(http.MethodPut, "/photos", nil, nil)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("create bucket: %s: %s", response.Status, readBody(response))
	}
	_ = response.Body.Close()
	response = request(http.MethodPut, "/photos/hello.txt", []byte("hello world"), map[string]string{"Content-Type": "text/plain", "X-Amz-Meta-Color": "blue"})
	if response.StatusCode != http.StatusOK || response.Header.Get("ETag") == "" {
		t.Fatalf("put: %s: %s", response.Status, readBody(response))
	}
	_ = response.Body.Close()
	response = request(http.MethodGet, "/photos/hello.txt", nil, map[string]string{"Range": "bytes=6-"})
	if response.StatusCode != http.StatusPartialContent || readBody(response) != "world" || response.Header.Get("X-Amz-Meta-Color") != "blue" {
		t.Fatalf("range get: status=%s body=%q metadata=%q", response.Status, readBody(response), response.Header.Get("X-Amz-Meta-Color"))
	}
	response = request(http.MethodGet, "/photos?list-type=2", nil, nil)
	body := readBody(response)
	if response.StatusCode != http.StatusOK || !strings.Contains(body, "<Key>hello.txt</Key>") {
		t.Fatalf("list: %s: %s", response.Status, body)
	}
	response = request(http.MethodDelete, "/photos/hello.txt", nil, nil)
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("delete: %s: %s", response.Status, readBody(response))
	}
	_ = response.Body.Close()
}

func readBody(response *http.Response) string {
	data, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	return string(data)
}
