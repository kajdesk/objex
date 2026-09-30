package local

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/kajdesk/objex/internal/storage"
)

func TestObjectLifecycle(t *testing.T) {
	t.Parallel()
	store, err := Open(t.TempDir(), false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()
	if err := store.CreateBucket(ctx, "photos", false); err != nil {
		t.Fatal(err)
	}
	created, err := store.PutObject(ctx, "photos", "a/hello.txt", bytes.NewBufferString("hello"), storage.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if created.Size != 5 || created.ETag != "5d41402abc4b2a76b9719d911017c592" {
		t.Fatalf("unexpected object: %+v", created)
	}
	got, file, err := store.OpenObject(ctx, "photos", "a/hello.txt")
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(file)
	_ = file.Close()
	if err != nil || string(data) != "hello" || got.CRC32C == 0 {
		t.Fatalf("read: data=%q object=%+v err=%v", data, got, err)
	}
	listed, err := store.ListObjects(ctx, "photos", storage.ListOptions{Prefix: "a/", Limit: 1000})
	if err != nil || len(listed.Objects) != 1 || listed.Objects[0].Key != "a/hello.txt" {
		t.Fatalf("list: %+v err=%v", listed, err)
	}
	_, err = store.PutObject(ctx, "photos", "a/hello.txt", bytes.NewBufferString("no"), storage.PutOptions{IfNoneMatch: "*"})
	if !errors.Is(err, storage.ErrPreconditionFailed) {
		t.Fatalf("conditional put: %v", err)
	}
	if err := store.DeleteObject(ctx, "photos", "a/hello.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.HeadObject(ctx, "photos", "a/hello.txt"); !errors.Is(err, storage.ErrNoSuchKey) {
		t.Fatalf("head after delete: %v", err)
	}
	if err := store.DeleteBucket(ctx, "photos"); err != nil {
		t.Fatal(err)
	}
}

func TestDelimiterListing(t *testing.T) {
	t.Parallel()
	store, err := Open(t.TempDir(), false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()
	_ = store.CreateBucket(ctx, "bucket", false)
	for _, key := range []string{"a", "dir/1", "dir/2", "other/1"} {
		if _, err := store.PutObject(ctx, "bucket", key, bytes.NewBufferString("x"), storage.PutOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	result, err := store.ListObjects(ctx, "bucket", storage.ListOptions{Delimiter: "/", Limit: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Objects) != 1 || len(result.Prefixes) != 2 || result.Prefixes[0] != "dir/" || result.Prefixes[1] != "other/" {
		t.Fatalf("unexpected listing: %+v", result)
	}
}
