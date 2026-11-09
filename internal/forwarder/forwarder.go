// Package forwarder streams a Railway environment's deploy and HTTP logs to Sazabi.
package forwarder

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/sazabi/railway-forwarder/internal/otlp"
	"github.com/sazabi/railway-forwarder/internal/railway"
)

// Railway is the subset of the Railway client the forwarder uses.
type Railway interface {
	FetchEnvironment(ctx context.Context, environmentID string) (*railway.Environment, error)
	Subscribe(ctx context.Context, query string, variables map[string]any, onData func(json.RawMessage) error) error
}

// Sink receives records bound for Sazabi.
type Sink interface {
	Add(ctx context.Context, records ...otlp.Record) error
}

type Options struct {
	EnvironmentID string
	// SelfServiceID is the forwarder's own service, whose logs are not forwarded.
	SelfServiceID   string
	DeployLogs      bool
	HTTPLogs        bool
	RefreshInterval time.Duration
}

type Forwarder struct {
	rw   Railway
	sink Sink
	opts Options
	log  *slog.Logger

	mu          sync.RWMutex
	env         *railway.Environment
	refreshMu   sync.Mutex
	lastRefresh time.Time
	changed     chan struct{}
	started     time.Time
}

func New(rw Railway, sink Sink, opts Options, log *slog.Logger) *Forwarder {
	if opts.RefreshInterval <= 0 {
		opts.RefreshInterval = 30 * time.Second
	}
	return &Forwarder{rw: rw, sink: sink, opts: opts, log: log, changed: make(chan struct{}, 1), started: time.Now().UTC()}
}

// Run loads the environment and streams logs until ctx ends. It fails fast when the
// environment cannot be loaded at startup, which almost always means a bad token.
func (f *Forwarder) Run(ctx context.Context) error {
	env, err := f.rw.FetchEnvironment(ctx, f.opts.EnvironmentID)
	if err != nil {
		return err
	}
	f.setEnvironment(env)
	f.log.Info("forwarding railway logs",
		"project", env.ProjectName,
		"environment", env.Name,
		"services", len(f.forwardedServices(env)),
		"deploy_logs", f.opts.DeployLogs,
		"http_logs", f.opts.HTTPLogs,
	)

	var wg sync.WaitGroup
	if f.opts.DeployLogs {
		wg.Go(func() { f.streamDeployLogs(ctx) })
	}
	if f.opts.HTTPLogs {
		wg.Go(func() { f.manageHTTPLogs(ctx) })
	}
	wg.Go(func() { f.refreshLoop(ctx) })
	wg.Wait()
	return nil
}

func (f *Forwarder) environment() *railway.Environment {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.env
}

func (f *Forwarder) setEnvironment(env *railway.Environment) {
	f.mu.Lock()
	f.env = env
	f.mu.Unlock()
	select {
	case f.changed <- struct{}{}:
	default:
	}
}

func (f *Forwarder) forwardedServices(env *railway.Environment) map[string]string {
	out := make(map[string]string, len(env.Services))
	for id, name := range env.Services {
		if id != f.opts.SelfServiceID {
			out[id] = name
		}
	}
	return out
}

// refresh reloads the environment, at most once every five seconds, so new
// services and deployments are picked up.
func (f *Forwarder) refresh(ctx context.Context) {
	f.refreshMu.Lock()
	defer f.refreshMu.Unlock()
	if time.Since(f.lastRefresh) < 5*time.Second {
		return
	}
	f.lastRefresh = time.Now()
	env, err := f.rw.FetchEnvironment(ctx, f.opts.EnvironmentID)
	if err != nil {
		if ctx.Err() == nil {
			f.log.Warn("refresh railway environment", "error", err)
		}
		return
	}
	prev := f.environment()
	for id, name := range env.Services {
		if _, ok := prev.Services[id]; !ok && id != f.opts.SelfServiceID {
			f.log.Info("discovered railway service", "service", name, "service_id", id)
		}
	}
	f.setEnvironment(env)
}

func (f *Forwarder) refreshLoop(ctx context.Context) {
	ticker := time.NewTicker(f.opts.RefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			f.refresh(ctx)
		}
	}
}

func (f *Forwarder) resource(serviceID, deploymentID, instanceID string) otlp.Resource {
	env := f.environment()
	name, ok := env.Services[serviceID]
	if !ok {
		name = serviceID
	}
	return otlp.Resource{
		ServiceName:          name,
		EnvironmentName:      env.Name,
		ProjectName:          env.ProjectName,
		ProjectID:            env.ProjectID,
		EnvironmentID:        env.ID,
		ServiceID:            serviceID,
		DeploymentID:         deploymentID,
		DeploymentInstanceID: instanceID,
	}
}

// resubscribe runs subscribe in a loop with capped exponential backoff, resetting the
// backoff after a subscription that stayed up for a while.
func (f *Forwarder) resubscribe(ctx context.Context, kind string, attrs []any, subscribe func(context.Context) error) {
	const minDelay, maxDelay = time.Second, 30 * time.Second
	delay := minDelay
	for ctx.Err() == nil {
		started := time.Now()
		err := subscribe(ctx)
		if ctx.Err() != nil {
			return
		}
		if time.Since(started) > time.Minute {
			delay = minDelay
		}
		args := append([]any{"kind", kind, "error", err, "retry_in", delay.String()}, attrs...)
		if railway.IsNotAuthorized(err) {
			f.log.Error("railway rejected RAILWAY_API_TOKEN; it must be an account token", args...)
		} else {
			f.log.Warn("railway log subscription ended", args...)
		}
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return
		}
		delay = min(delay*2, maxDelay)
	}
}
