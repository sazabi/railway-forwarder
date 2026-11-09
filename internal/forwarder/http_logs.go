package forwarder

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/sazabi/railway-forwarder/internal/otlp"
)

const httpLogsSubscription = `subscription forwarderHttpLogs($deploymentId: String!, $filter: String, $beforeLimit: Int!, $beforeDate: String) {
  httpLogs(deploymentId: $deploymentId, filter: $filter, beforeLimit: $beforeLimit, beforeDate: $beforeDate) {
    requestId
    timestamp
    method
    path
    host
    httpStatus
    upstreamProto
    downstreamProto
    responseDetails
    totalDuration
    upstreamAddress
    clientUa
    upstreamRqDuration
    txBytes
    rxBytes
    srcIp
    edgeRegion
  }
}`

// removedDeploymentGrace keeps following a deployment after Railway replaces it, so
// requests it was still draining are not lost.
const removedDeploymentGrace = 2 * time.Minute

type httpLog struct {
	RequestID          string    `json:"requestId"`
	Timestamp          time.Time `json:"timestamp"`
	Method             string    `json:"method"`
	Path               string    `json:"path"`
	Host               string    `json:"host"`
	HTTPStatus         int64     `json:"httpStatus"`
	UpstreamProto      string    `json:"upstreamProto"`
	DownstreamProto    string    `json:"downstreamProto"`
	ResponseDetails    string    `json:"responseDetails"`
	TotalDuration      int64     `json:"totalDuration"`
	UpstreamAddress    string    `json:"upstreamAddress"`
	ClientUA           string    `json:"clientUa"`
	UpstreamRqDuration int64     `json:"upstreamRqDuration"`
	TxBytes            int64     `json:"txBytes"`
	RxBytes            int64     `json:"rxBytes"`
	SrcIP              string    `json:"srcIp"`
	EdgeRegion         string    `json:"edgeRegion"`
}

type httpLogsData struct {
	HTTPLogs []httpLog `json:"httpLogs"`
}

// httpBody is the record body. Its keys are the ones Sazabi's Railway intake reads
// request fields from.
type httpBody struct {
	Method             string `json:"method"`
	Path               string `json:"path"`
	Host               string `json:"host,omitempty"`
	Status             int64  `json:"status"`
	DurationMs         int64  `json:"durationMs"`
	IP                 string `json:"ip,omitempty"`
	UserAgent          string `json:"userAgent,omitempty"`
	RequestID          string `json:"requestId,omitempty"`
	EdgeRegion         string `json:"edgeRegion,omitempty"`
	UpstreamAddress    string `json:"upstreamAddress,omitempty"`
	UpstreamDurationMs int64  `json:"upstreamDurationMs"`
	UpstreamProto      string `json:"upstreamProto,omitempty"`
	DownstreamProto    string `json:"downstreamProto,omitempty"`
	ResponseDetails    string `json:"responseDetails,omitempty"`
	TxBytes            int64  `json:"txBytes"`
	RxBytes            int64  `json:"rxBytes"`
}

type httpSubscription struct {
	cancel    context.CancelFunc
	serviceID string
	removedAt time.Time
}

// manageHTTPLogs keeps one HTTP log subscription per live deployment of every
// service with a public domain, following deploys and newly added services.
func (f *Forwarder) manageHTTPLogs(ctx context.Context) {
	var wg sync.WaitGroup
	defer wg.Wait()
	subs := make(map[string]*httpSubscription)
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	reconcile := func() {
		env := f.environment()
		now := time.Now()
		for id, serviceID := range env.HTTPDeployments {
			if serviceID == f.opts.SelfServiceID {
				continue
			}
			if s, ok := subs[id]; ok {
				s.removedAt = time.Time{}
				continue
			}
			sctx, cancel := context.WithCancel(ctx)
			subs[id] = &httpSubscription{cancel: cancel, serviceID: serviceID}
			f.log.Info("following http logs", "service", env.Services[serviceID], "deployment_id", id)
			wg.Go(func() { f.streamHTTPLogs(sctx, id, serviceID) })
		}
		for id, s := range subs {
			if _, ok := env.HTTPDeployments[id]; ok {
				continue
			}
			if s.removedAt.IsZero() {
				s.removedAt = now
			} else if now.Sub(s.removedAt) >= removedDeploymentGrace {
				s.cancel()
				delete(subs, id)
			}
		}
	}

	reconcile()
	for {
		select {
		case <-ctx.Done():
			return
		case <-f.changed:
			reconcile()
		case <-ticker.C:
			reconcile()
		}
	}
}

func (f *Forwarder) streamHTTPLogs(ctx context.Context, deploymentID, serviceID string) {
	cursor := f.started
	f.resubscribe(ctx, "http", []any{"deployment_id", deploymentID}, func(ctx context.Context) error {
		vars := map[string]any{
			"deploymentId": deploymentID,
			"filter":       "",
			"beforeLimit":  5000,
			"beforeDate":   cursor.Format(time.RFC3339Nano),
		}
		return f.rw.Subscribe(ctx, httpLogsSubscription, vars, func(data json.RawMessage) error {
			var payload httpLogsData
			if err := json.Unmarshal(data, &payload); err != nil {
				f.log.Warn("decode railway http logs", "error", err)
				return nil
			}
			records := make([]otlp.Record, 0, len(payload.HTTPLogs))
			for _, l := range payload.HTTPLogs {
				if !l.Timestamp.After(cursor) {
					continue
				}
				cursor = l.Timestamp
				records = append(records, f.httpRecord(l, serviceID, deploymentID))
			}
			if len(records) == 0 {
				return nil
			}
			return f.sink.Add(ctx, records...)
		})
	})
}

func httpSeverity(status int64) (int, string) {
	switch {
	case status >= 500:
		return otlp.Severity("error")
	case status >= 400:
		return otlp.Severity("warn")
	default:
		return otlp.Severity("info")
	}
}

func (f *Forwarder) httpRecord(l httpLog, serviceID, deploymentID string) otlp.Record {
	body, _ := json.Marshal(httpBody{
		Method:             l.Method,
		Path:               l.Path,
		Host:               l.Host,
		Status:             l.HTTPStatus,
		DurationMs:         l.TotalDuration,
		IP:                 l.SrcIP,
		UserAgent:          l.ClientUA,
		RequestID:          l.RequestID,
		EdgeRegion:         l.EdgeRegion,
		UpstreamAddress:    l.UpstreamAddress,
		UpstreamDurationMs: l.UpstreamRqDuration,
		UpstreamProto:      l.UpstreamProto,
		DownstreamProto:    l.DownstreamProto,
		ResponseDetails:    l.ResponseDetails,
		TxBytes:            l.TxBytes,
		RxBytes:            l.RxBytes,
	})
	attrs := []otlp.KeyValue{
		otlp.String("railway.log.type", "http"),
		otlp.String("http.request.method", l.Method),
		otlp.String("url.path", l.Path),
		otlp.Int("http.response.status_code", l.HTTPStatus),
		otlp.Int("duration_ms", l.TotalDuration),
	}
	optional := map[string]string{
		"server.address":      l.Host,
		"client.address":      l.SrcIP,
		"user_agent.original": l.ClientUA,
		"railway.request.id":  l.RequestID,
		"railway.edge.region": l.EdgeRegion,
	}
	for _, k := range []string{"server.address", "client.address", "user_agent.original", "railway.request.id", "railway.edge.region"} {
		if v := optional[k]; v != "" {
			attrs = append(attrs, otlp.String(k, v))
		}
	}
	num, text := httpSeverity(l.HTTPStatus)
	return otlp.Record{
		Resource:       f.resource(serviceID, deploymentID, ""),
		Time:           l.Timestamp,
		SeverityNumber: num,
		SeverityText:   text,
		Body:           string(body),
		Attributes:     attrs,
	}
}
