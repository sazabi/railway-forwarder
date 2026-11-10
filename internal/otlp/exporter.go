package otlp

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	maxBatch      = 1000
	flushInterval = 2 * time.Second
	maxAttempts   = 6
	shutdownGrace = 10 * time.Second
)

// Exporter batches records and POSTs them to an OTLP/HTTP logs endpoint.
type Exporter struct {
	endpoint string
	version  string
	client   *http.Client
	queue    chan Record
	done     chan struct{}
	log      *slog.Logger
	backoff  time.Duration
}

// NewExporter sends to `<intakeURL>/v1/logs`.
func NewExporter(intakeURL, version string, log *slog.Logger) *Exporter {
	return &Exporter{
		endpoint: strings.TrimRight(intakeURL, "/") + "/v1/logs",
		version:  version,
		client:   &http.Client{Timeout: 30 * time.Second},
		queue:    make(chan Record, 4*maxBatch),
		done:     make(chan struct{}),
		log:      log,
		backoff:  500 * time.Millisecond,
	}
}

// Add enqueues records, blocking while the queue is full so a slow intake applies
// backpressure to the Railway subscriptions instead of dropping logs.
func (e *Exporter) Add(ctx context.Context, records ...Record) error {
	for _, r := range records {
		select {
		case e.queue <- r:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// Run flushes batches until ctx ends, then drains what is queued.
func (e *Exporter) Run(ctx context.Context) {
	defer close(e.done)
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()
	batch := make([]Record, 0, maxBatch)
	flush := func(fctx context.Context) {
		if len(batch) == 0 {
			return
		}
		e.send(fctx, batch)
		batch = batch[:0]
	}
	for {
		select {
		case r := <-e.queue:
			batch = append(batch, r)
			if len(batch) >= maxBatch {
				flush(ctx)
			}
		case <-ticker.C:
			flush(ctx)
		case <-ctx.Done():
			sctx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
			defer cancel()
			for {
				select {
				case r := <-e.queue:
					batch = append(batch, r)
					if len(batch) >= maxBatch {
						flush(sctx)
					}
				default:
					flush(sctx)
					return
				}
			}
		}
	}
}

// Wait blocks until Run has returned.
func (e *Exporter) Wait() { <-e.done }

func (e *Exporter) send(ctx context.Context, batch []Record) {
	payload, err := Encode(batch, e.version, time.Now())
	if err != nil {
		e.log.Error("encode otlp batch", "error", err, "records", len(batch))
		return
	}
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, _ = zw.Write(payload)
	_ = zw.Close()
	body := buf.Bytes()

	delay := e.backoff
	for attempt := 1; ; attempt++ {
		retryAfter, err := e.post(ctx, body)
		if err == nil {
			return
		}
		var perm permanentError
		if asPermanent(err, &perm) || attempt >= maxAttempts || ctx.Err() != nil {
			e.log.Error("dropped log batch", "error", err, "records", len(batch), "attempts", attempt)
			return
		}
		wait := delay + rand.N(delay/2+1)
		if retryAfter > wait {
			wait = retryAfter
		}
		e.log.Warn("retrying log batch", "error", err, "records", len(batch), "attempt", attempt, "wait", wait.String())
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			e.log.Error("dropped log batch", "error", ctx.Err(), "records", len(batch), "attempts", attempt)
			return
		}
		delay = min(delay*2, 30*time.Second)
	}
}

type permanentError struct{ status int }

func (p permanentError) Error() string {
	return fmt.Sprintf("intake rejected batch: status %d", p.status)
}

func asPermanent(err error, target *permanentError) bool {
	p, ok := err.(permanentError)
	if ok {
		*target = p
	}
	return ok
}

func (e *Exporter) post(ctx context.Context, body []byte) (time.Duration, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.endpoint, bytes.NewReader(body))
	if err != nil {
		return 0, permanentError{}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", "gzip")
	req.Header.Set("User-Agent", "sazabi-railway-forwarder/"+e.version)
	resp, err := e.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return 0, nil
	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 || resp.StatusCode == http.StatusRequestTimeout:
		return parseRetryAfter(resp.Header.Get("Retry-After")), fmt.Errorf("intake status %d", resp.StatusCode)
	default:
		return 0, permanentError{status: resp.StatusCode}
	}
}

func parseRetryAfter(v string) time.Duration {
	if secs, err := strconv.Atoi(strings.TrimSpace(v)); err == nil && secs > 0 {
		return min(time.Duration(secs)*time.Second, time.Minute)
	}
	return 0
}
