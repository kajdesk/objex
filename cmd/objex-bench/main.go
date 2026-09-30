package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kajdesk/objex/pkg/sigv4"
)

type workload struct {
	name        string
	count       int
	size        int
	concurrency int
}

func main() {
	endpoint := flag.String("endpoint", "http://127.0.0.1:9000", "objex S3 endpoint")
	access := flag.String("access-key", "", "access key")
	secret := flag.String("secret-key", "", "secret key")
	bucket := flag.String("bucket", "bench", "benchmark bucket")
	quick := flag.Bool("quick", false, "run a smaller smoke benchmark")
	flag.Parse()
	if *access == "" || *secret == "" {
		fmt.Fprintln(os.Stderr, "-access-key and -secret-key are required")
		os.Exit(2)
	}
	base, err := url.Parse(strings.TrimRight(*endpoint, "/"))
	if err != nil {
		panic(err)
	}
	client := &http.Client{Transport: &http.Transport{
		MaxIdleConns: 512, MaxIdleConnsPerHost: 512, IdleConnTimeout: 30 * time.Second,
	}}
	signer := sigv4.Signer{Credentials: sigv4.Credentials{AccessKey: *access, SecretKey: *secret}, Region: "auto", Service: "s3"}
	if err := createBucket(client, signer, base, *bucket); err != nil {
		panic(err)
	}
	loads := []workload{{"small", 5000, 16 << 10, 64}, {"medium", 400, 1 << 20, 32}, {"large", 16, 64 << 20, 8}}
	if *quick {
		loads = []workload{{"small", 500, 16 << 10, 32}, {"medium", 40, 1 << 20, 16}, {"large", 2, 16 << 20, 2}}
	}
	for _, load := range loads {
		data := make([]byte, load.size)
		for i := range data {
			data[i] = byte(i * 7)
		}
		for _, method := range []string{http.MethodPut, http.MethodGet} {
			if err := run(client, signer, base, *bucket, load, method, data); err != nil {
				panic(err)
			}
		}
	}
}

func run(client *http.Client, signer sigv4.Signer, base *url.URL, bucket string, load workload, method string, data []byte) error {
	start := time.Now()
	jobs := make(chan int)
	var failed atomic.Int64
	var firstErr atomic.Value
	var workers sync.WaitGroup
	for range load.concurrency {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for index := range jobs {
				key := fmt.Sprintf("%s/%d", load.name, index)
				if err := request(client, signer, base, bucket, key, method, data); err != nil {
					failed.Add(1)
					if firstErr.Load() == nil {
						firstErr.Store(err.Error())
					}
				}
			}
		}()
	}
	for i := range load.count {
		jobs <- i
	}
	close(jobs)
	workers.Wait()
	if failed.Load() != 0 {
		return fmt.Errorf("%d requests failed: %v", failed.Load(), firstErr.Load())
	}
	seconds := time.Since(start).Seconds()
	fmt.Printf("%8s %s: %d x %d KiB, %d parallel: %8.0f ops/s %8.1f MiB/s\n",
		load.name, method, load.count, load.size/1024, load.concurrency,
		float64(load.count)/seconds, float64(load.count*load.size)/seconds/(1<<20))
	return nil
}

func request(client *http.Client, signer sigv4.Signer, base *url.URL, bucket, key, method string, data []byte) error {
	target := *base
	target.Path = strings.TrimRight(base.Path, "/") + "/" + bucket + "/" + key
	var body io.Reader
	payload := emptyHash()
	if method == http.MethodPut {
		body = bytes.NewReader(data)
		sum := sha256.Sum256(data)
		payload = hex.EncodeToString(sum[:])
	}
	req, err := http.NewRequestWithContext(context.Background(), method, target.String(), body)
	if err != nil {
		return err
	}
	if err := signer.Sign(req, payload); err != nil {
		return err
	}
	response, err := client.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode/100 != 2 {
		message, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return fmt.Errorf("%s %s: %s: %s", method, key, response.Status, message)
	}
	if method == http.MethodGet {
		count, err := io.Copy(io.Discard, response.Body)
		if err != nil {
			return err
		}
		if count != int64(len(data)) {
			return fmt.Errorf("%s: got %d bytes, want %d", key, count, len(data))
		}
	}
	return nil
}

func createBucket(client *http.Client, signer sigv4.Signer, base *url.URL, bucket string) error {
	target := *base
	target.Path = strings.TrimRight(base.Path, "/") + "/" + bucket
	req, _ := http.NewRequest(http.MethodPut, target.String(), nil)
	if err := signer.Sign(req, emptyHash()); err != nil {
		return err
	}
	response, err := client.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode/100 == 2 || response.StatusCode == http.StatusConflict {
		return nil
	}
	message, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
	return fmt.Errorf("create bucket: %s: %s", response.Status, message)
}

func emptyHash() string {
	sum := sha256.Sum256(nil)
	return hex.EncodeToString(sum[:])
}
