package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type payload struct {
	FormID string            `json:"form_id"`
	Key    string            `json:"idempotency_key"`
	Email  string            `json:"email"`
	Data   map[string]string `json:"data"`
}

func main() {
	base := flag.String("url", "http://localhost:8080", "API base URL")
	total := flag.Int("n", 100000, "total requests")
	workers := flag.Int("workers", 100, "concurrent workers")
	flag.Parse()

	unique := *total * 80 / 100
	jobs := make(chan payload, *workers*2)
	var created, duplicates, errors, totalRequests int64

	// Reuse persistent TCP connections across all workers to prevent Windows ephemeral port exhaustion
	client := &http.Client{
		Timeout: 15 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        *workers * 2,
			MaxIdleConnsPerHost: *workers * 2,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for p := range jobs {
				b, _ := json.Marshal(p)
				req, err := http.NewRequest(http.MethodPost, *base+"/submissions", bytes.NewReader(b))
				if err != nil {
					atomic.AddInt64(&errors, 1)
					continue
				}
				req.Header.Set("Content-Type", "application/json")

				resp, err := client.Do(req)
				if err != nil {
					atomic.AddInt64(&errors, 1)
					continue
				}
				// Drain body completely so Go's HTTP transport reuses the TCP connection
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()

				atomic.AddInt64(&totalRequests, 1)
				switch resp.StatusCode {
				case 201:
					atomic.AddInt64(&created, 1)
				case 200:
					atomic.AddInt64(&duplicates, 1)
				default:
					atomic.AddInt64(&errors, 1)
				}
			}
		}()
	}

	for i := 0; i < unique; i++ {
		jobs <- payload{
			FormID: "load-test-form",
			Key:    fmt.Sprintf("load-key-%d", i),
			Email:  fmt.Sprintf("user%d@example.com", i),
			Data:   map[string]string{"name": fmt.Sprintf("User %d", i)},
		}
	}
	for i := 0; i < *total-unique; i++ {
		// Reuse the first 20,000 keys; each of these requests is a duplicate.
		jobs <- payload{
			FormID: "load-test-form",
			Key:    fmt.Sprintf("load-key-%d", i%unique),
			Email:  fmt.Sprintf("user%d@example.com", i%unique),
			Data:   map[string]string{"name": "duplicate"},
		}
	}
	close(jobs)
	wg.Wait()

	elapsed := time.Since(start)
	fmt.Printf("Requests completed: %d\nCreated (201): %d\nDuplicates (200): %d\nErrors: %d\nDuration: %s\nThroughput: %.2f req/s\n",
		totalRequests, created, duplicates, errors, elapsed, float64(totalRequests)/elapsed.Seconds())
}