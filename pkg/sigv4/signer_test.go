package sigv4

import (
	"net/http"
	"testing"
	"time"
)

func TestSignDeterministic(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "https://example.com/bucket/a%20b?prefix=a%2Fb&max-keys=10", nil)
	if err != nil {
		t.Fatal(err)
	}
	signer := Signer{
		Credentials: Credentials{AccessKey: "AKID", SecretKey: "secret"},
		Region:      "auto", Service: "s3",
		Now: func() time.Time { return time.Date(2026, 9, 30, 1, 2, 3, 0, time.UTC) },
	}
	if err := signer.Sign(req, "UNSIGNED-PAYLOAD"); err != nil {
		t.Fatal(err)
	}
	want := "AWS4-HMAC-SHA256 Credential=AKID/20260930/auto/s3/aws4_request,SignedHeaders=host;x-amz-content-sha256;x-amz-date,Signature="
	if got := req.Header.Get("Authorization"); len(got) != len(want)+64 || got[:len(want)] != want {
		t.Fatalf("unexpected authorization: %s", got)
	}
}
