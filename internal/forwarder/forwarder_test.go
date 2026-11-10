package forwarder

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sazabi/railway-forwarder/internal/otlp"
	"github.com/sazabi/railway-forwarder/internal/railway"
)

type fakeRailway struct {
	mu        sync.Mutex
	env       *railway.Environment
	fetches   int
	deploy    []string
	http      map[string][]string
	subscribe chan string
	// onDeploySubscribe runs before the deploy batches are delivered.
	onDeploySubscribe func()
}

func (f *fakeRailway) FetchEnvironment(context.Context, string) (*railway.Environment, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fetches++
	cp := *f.env
	return &cp, nil
}

func (f *fakeRailway) Subscribe(ctx context.Context, query string, vars map[string]any, onData func(json.RawMessage) error) error {
	var batches []string
	switch {
	case strings.Contains(query, "environmentLogs"):
		batches = f.deploy
		if f.onDeploySubscribe != nil {
			f.onDeploySubscribe()
		}
		f.subscribe <- "deploy"
	case strings.Contains(query, "httpLogs"):
		id := vars["deploymentId"].(string)
		batches = f.http[id]
		f.subscribe <- "http:" + id
	}
	for _, b := range batches {
		if err := onData(json.RawMessage(b)); err != nil {
			return err
		}
	}
	<-ctx.Done()
	return ctx.Err()
}

type sink struct {
	mu      sync.Mutex
	records []otlp.Record
}

func (s *sink) Add(_ context.Context, r ...otlp.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = append(s.records, r...)
	return nil
}

func (s *sink) snapshot() []otlp.Record {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]otlp.Record(nil), s.records...)
}

func ts(d time.Duration) string { return time.Now().Add(d).UTC().Format(time.RFC3339Nano) }

func deployLog(at, service, severity, msg string, attrs string) string {
	return fmt.Sprintf(`{"timestamp":%q,"message":%q,"severity":%q,"tags":{"serviceId":%q,"deploymentId":"dep-%s","deploymentInstanceId":"inst-%s"},"attributes":[{"key":"level","value":"\"%s\""}%s]}`,
		at, msg, severity, service, service, service, severity, attrs)
}

func TestForwarderMapsDeployAndHTTPLogs(t *testing.T) {
	later := ts(2 * time.Second)
	rw := &fakeRailway{
		subscribe: make(chan string, 10),
		env: &railway.Environment{
			ID: "env", Name: "production", ProjectID: "proj", ProjectName: "shop",
			Services:        map[string]string{"web": "Web", "self": "Forwarder"},
			HTTPDeployments: map[string]string{"dep-web": "web"},
		},
		deploy: []string{`{"environmentLogs":[` + strings.Join([]string{
			deployLog(ts(time.Second), "web", "error", "boom", `,{"key":"userId","value":"42"}`),
			deployLog(ts(-time.Hour), "web", "info", "before start", ""),
			deployLog(ts(1500*time.Millisecond), "self", "info", "own log", ""),
			deployLog(ts(1600*time.Millisecond), "web", "info", "", ""),
			deployLog(time.Now().Add(time.Hour).Truncate(time.Second).UTC().Format(time.RFC3339Nano), "web", "info", "Starting Container", ""),
			deployLog(later, "new", "warn", "hello from a new service", ""),
		}, ",") + `]}`},
		http: map[string][]string{"dep-web": {fmt.Sprintf(`{"httpLogs":[
			{"requestId":"r1","timestamp":%q,"method":"GET","path":"/ok","host":"web.up.railway.app","httpStatus":200,"totalDuration":12,"srcIp":"1.2.3.4","clientUa":"curl"},
			{"requestId":"r2","timestamp":%q,"method":"POST","path":"/missing","httpStatus":404,"totalDuration":3},
			{"requestId":"r3","timestamp":%q,"method":"GET","path":"/fail","httpStatus":503,"totalDuration":30}]}`,
			ts(time.Second), ts(2*time.Second), ts(3*time.Second))}},
	}
	out := &sink{}
	f := New(rw, out, Options{EnvironmentID: "env", SelfServiceID: "self", DeployLogs: true, HTTPLogs: true}, slog.New(slog.DiscardHandler))

	// A service created after startup: it appears on the next environment fetch.
	rw.onDeploySubscribe = func() {
		rw.mu.Lock()
		rw.env.Services = map[string]string{"web": "Web", "self": "Forwarder", "new": "New"}
		rw.mu.Unlock()
	}
	f.lastRefresh = time.Now().Add(-time.Minute)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = f.Run(ctx); close(done) }()

	deadline := time.After(5 * time.Second)
	for len(out.snapshot()) < 5 {
		select {
		case <-deadline:
			t.Fatalf("got %d records: %+v", len(out.snapshot()), out.snapshot())
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
	<-done

	records := out.snapshot()
	byBody := map[string]otlp.Record{}
	for _, r := range records {
		byBody[r.Body] = r
	}
	if len(records) != 5 {
		t.Fatalf("records = %d: %v", len(records), byBody)
	}
	boom := byBody["boom"]
	if boom.SeverityNumber != 17 || boom.Resource.ServiceName != "Web" || boom.Resource.EnvironmentName != "production" ||
		boom.Resource.ProjectName != "shop" || boom.Resource.DeploymentID != "dep-web" || boom.Resource.DeploymentInstanceID != "inst-web" {
		t.Errorf("boom = %+v", boom)
	}
	if !hasAttr(boom, "railway.log.type", "deploy") || !hasAttr(boom, "userId", "42") || hasAttr(boom, "level", "") {
		t.Errorf("boom attrs = %+v", boom.Attributes)
	}
	if r := byBody["hello from a new service"]; r.Resource.ServiceName != "New" || r.SeverityText != "WARN" {
		t.Errorf("new service record = %+v", r)
	}

	var statuses []string
	for _, r := range records {
		if hasAttr(r, "railway.log.type", "http") {
			var body map[string]any
			if err := json.Unmarshal([]byte(r.Body), &body); err != nil {
				t.Fatal(err)
			}
			statuses = append(statuses, fmt.Sprintf("%v:%s", body["status"], r.SeverityText))
			if body["method"] == "" || r.Resource.ServiceName != "Web" || r.Resource.DeploymentID != "dep-web" {
				t.Errorf("http record = %+v", r)
			}
		}
	}
	if got := strings.Join(statuses, ","); got != "200:INFO,404:WARN,503:ERROR" {
		t.Errorf("http statuses = %s", got)
	}
}

func hasAttr(r otlp.Record, key, value string) bool {
	for _, a := range r.Attributes {
		if a.Key != key {
			continue
		}
		switch {
		case value == "":
			return true
		case a.Value.StringValue != nil:
			return *a.Value.StringValue == value
		case a.Value.IntValue != nil:
			return *a.Value.IntValue == value
		}
	}
	return false
}
