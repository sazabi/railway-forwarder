package otlp

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestSeverity(t *testing.T) {
	cases := map[string]struct {
		num  int
		text string
	}{
		"trace": {1, "TRACE"}, "DEBUG": {5, "DEBUG"}, "info": {9, "INFO"}, "": {9, "INFO"},
		"warn": {13, "WARN"}, "warning": {13, "WARN"}, "error": {17, "ERROR"}, "err": {17, "ERROR"},
		"fatal": {21, "FATAL"}, "critical": {21, "FATAL"}, "panic": {21, "FATAL"}, "notice": {9, "INFO"},
	}
	for in, want := range cases {
		num, text := Severity(in)
		if num != want.num || text != want.text {
			t.Errorf("Severity(%q) = %d %s, want %d %s", in, num, text, want.num, want.text)
		}
	}
}

func TestFromJSON(t *testing.T) {
	if v := FromJSON("k", `"hello"`); *v.Value.StringValue != "hello" {
		t.Errorf("string: %+v", v)
	}
	if v := FromJSON("k", `42`); *v.Value.IntValue != "42" {
		t.Errorf("int: %+v", v)
	}
	if v := FromJSON("k", `1.5`); *v.Value.DoubleValue != 1.5 {
		t.Errorf("double: %+v", v)
	}
	if v := FromJSON("k", `true`); !*v.Value.BoolValue {
		t.Errorf("bool: %+v", v)
	}
	if v := FromJSON("k", `{"a":1}`); *v.Value.StringValue != `{"a":1}` {
		t.Errorf("object: %+v", v)
	}
	if v := FromJSON("k", `not json`); *v.Value.StringValue != "not json" {
		t.Errorf("raw: %+v", v)
	}
}

func TestEncodeGroupsByResource(t *testing.T) {
	a := Resource{ServiceName: "api", EnvironmentName: "production", ProjectName: "shop", ServiceID: "s1"}
	b := Resource{ServiceName: "worker", EnvironmentName: "production", ProjectName: "shop", ServiceID: "s2"}
	ts := time.Unix(1700000000, 123)
	out, err := Encode([]Record{
		{Resource: a, Time: ts, SeverityNumber: 9, SeverityText: "INFO", Body: "one"},
		{Resource: b, Time: ts, SeverityNumber: 17, SeverityText: "ERROR", Body: "two"},
		{Resource: a, Time: ts, SeverityNumber: 9, SeverityText: "INFO", Body: "three"},
	}, "v1.0.0", ts)
	if err != nil {
		t.Fatal(err)
	}
	var req exportRequest
	if err := json.Unmarshal(out, &req); err != nil {
		t.Fatal(err)
	}
	if len(req.ResourceLogs) != 2 {
		t.Fatalf("resourceLogs = %d, want 2", len(req.ResourceLogs))
	}
	first := req.ResourceLogs[0]
	if got := len(first.ScopeLogs[0].LogRecords); got != 2 {
		t.Fatalf("api records = %d, want 2", got)
	}
	attrs := map[string]string{}
	for _, kv := range first.Resource.Attributes {
		attrs[kv.Key] = *kv.Value.StringValue
	}
	for k, want := range map[string]string{"service.name": "api", "deployment.environment.name": "production", "railway.project.name": "shop", "railway.service.id": "s1"} {
		if attrs[k] != want {
			t.Errorf("resource %s = %q, want %q", k, attrs[k], want)
		}
	}
	if _, ok := attrs["railway.deployment.id"]; ok {
		t.Error("empty attributes must be omitted")
	}
	if got := first.ScopeLogs[0].LogRecords[0].TimeUnixNano; got != "1700000000000000123" {
		t.Errorf("timeUnixNano = %s", got)
	}
}

func TestExporterRetriesAndSends(t *testing.T) {
	var calls atomic.Int32
	got := make(chan exportRequest, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if r.URL.Path != "/v1/logs" || r.Header.Get("Content-Encoding") != "gzip" || r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("unexpected request %s %v", r.URL.Path, r.Header)
		}
		zr, err := gzip.NewReader(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		raw, _ := io.ReadAll(zr)
		var req exportRequest
		if err := json.Unmarshal(raw, &req); err != nil {
			t.Error(err)
		}
		got <- req
	}))
	defer srv.Close()

	e := NewExporter(srv.URL+"/", "test", slog.New(slog.DiscardHandler))
	e.backoff = time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	go e.Run(ctx)
	if err := e.Add(ctx, Record{Resource: Resource{ServiceName: "api"}, Time: time.Now(), Body: "hi"}); err != nil {
		t.Fatal(err)
	}
	select {
	case req := <-got:
		if n := len(req.ResourceLogs[0].ScopeLogs[0].LogRecords); n != 1 {
			t.Errorf("records = %d", n)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no export received")
	}
	cancel()
	e.Wait()
	// Delivery is at least once: cancelling before the 200 arrives resends.
	if calls.Load() < 2 {
		t.Errorf("calls = %d, want at least 2", calls.Load())
	}
}

func TestExporterDoesNotRetryClientErrors(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()
	e := NewExporter(srv.URL, "test", slog.New(slog.DiscardHandler))
	e.backoff = time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	go e.Run(ctx)
	_ = e.Add(ctx, Record{Time: time.Now(), Body: "hi"})
	cancel()
	e.Wait()
	if calls.Load() != 1 {
		t.Errorf("calls = %d, want 1", calls.Load())
	}
}

func TestExporterResendsInFlightBatchOnShutdown(t *testing.T) {
	var calls atomic.Int32
	inFlight := make(chan struct{})
	delivered := make(chan struct{}, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if calls.Add(1) == 1 {
			close(inFlight)
			<-r.Context().Done()
			return
		}
		delivered <- struct{}{}
	}))
	defer srv.Close()

	e := NewExporter(srv.URL, "test", slog.New(slog.DiscardHandler))
	ctx, cancel := context.WithCancel(context.Background())
	go e.Run(ctx)
	for range maxBatch {
		if err := e.Add(ctx, Record{Time: time.Now(), Body: "x"}); err != nil {
			t.Fatal(err)
		}
	}
	<-inFlight
	cancel()
	e.Wait()
	select {
	case <-delivered:
	default:
		t.Fatal("the in-flight batch was not resent during shutdown")
	}
}

func TestExporterStopsDrainingWhenShutdownGraceExpires(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		calls.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	e := NewExporter(srv.URL, "test", slog.New(slog.DiscardHandler))
	e.backoff = 20 * time.Millisecond
	e.grace = 100 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	for range 3 * maxBatch {
		if err := e.Add(ctx, Record{Time: time.Now(), Body: "x"}); err != nil {
			t.Fatal(err)
		}
	}
	cancel()
	start := time.Now()
	e.Run(ctx)
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("shutdown took %s, want it bounded by the grace period", elapsed)
	}
	if calls.Load() == 0 {
		t.Fatal("the drain never tried to deliver")
	}
}
