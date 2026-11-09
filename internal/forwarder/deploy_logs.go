package forwarder

import (
	"context"
	"encoding/json"
	"time"

	"github.com/sazabi/railway-forwarder/internal/otlp"
)

const environmentLogsSubscription = `subscription forwarderEnvironmentLogs($environmentId: String!, $filter: String, $beforeLimit: Int!, $beforeDate: String) {
  environmentLogs(environmentId: $environmentId, filter: $filter, beforeLimit: $beforeLimit, beforeDate: $beforeDate) {
    timestamp
    message
    severity
    tags {
      projectId
      environmentId
      serviceId
      deploymentId
      deploymentInstanceId
    }
    attributes { key value }
  }
}`

type environmentLog struct {
	Timestamp time.Time `json:"timestamp"`
	Message   string    `json:"message"`
	Severity  string    `json:"severity"`
	Tags      struct {
		ServiceID            string `json:"serviceId"`
		DeploymentID         string `json:"deploymentId"`
		DeploymentInstanceID string `json:"deploymentInstanceId"`
	} `json:"tags"`
	Attributes []struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	} `json:"attributes"`
}

type environmentLogsData struct {
	EnvironmentLogs []environmentLog `json:"environmentLogs"`
}

// streamDeployLogs follows every service's deploy (runtime) logs in the environment.
// The subscription is unfiltered, so services added later are included without
// resubscribing.
func (f *Forwarder) streamDeployLogs(ctx context.Context) {
	cursor := time.Now().UTC()
	f.resubscribe(ctx, "deploy", nil, func(ctx context.Context) error {
		vars := map[string]any{
			"environmentId": f.opts.EnvironmentID,
			"filter":        "",
			"beforeLimit":   5000,
			"beforeDate":    cursor.Format(time.RFC3339Nano),
		}
		return f.rw.Subscribe(ctx, environmentLogsSubscription, vars, func(data json.RawMessage) error {
			var payload environmentLogsData
			if err := json.Unmarshal(data, &payload); err != nil {
				f.log.Warn("decode railway deploy logs", "error", err)
				return nil
			}
			records := f.deployRecords(ctx, payload.EnvironmentLogs, &cursor)
			if len(records) == 0 {
				return nil
			}
			return f.sink.Add(ctx, records...)
		})
	})
}

func (f *Forwarder) deployRecords(ctx context.Context, logs []environmentLog, cursor *time.Time) []otlp.Record {
	records := make([]otlp.Record, 0, len(logs))
	for _, l := range logs {
		// Logs without a message carry only their level attribute.
		if l.Message == "" && len(l.Attributes) <= 1 {
			continue
		}
		// Railway's own container lifecycle lines have whole-second timestamps.
		if l.Timestamp.Nanosecond() == 0 {
			continue
		}
		if !l.Timestamp.After(*cursor) {
			continue
		}
		*cursor = l.Timestamp
		if l.Tags.ServiceID == "" || l.Tags.ServiceID == f.opts.SelfServiceID {
			continue
		}
		if _, known := f.environment().Services[l.Tags.ServiceID]; !known {
			f.refresh(ctx)
		}
		records = append(records, f.deployRecord(l))
	}
	return records
}

func (f *Forwarder) deployRecord(l environmentLog) otlp.Record {
	level := l.Severity
	attrs := make([]otlp.KeyValue, 0, len(l.Attributes)+1)
	attrs = append(attrs, otlp.String("railway.log.type", "deploy"))
	for _, a := range l.Attributes {
		if a.Key == "level" {
			continue
		}
		attrs = append(attrs, otlp.FromJSON(a.Key, a.Value))
	}
	num, text := otlp.Severity(level)
	return otlp.Record{
		Resource:       f.resource(l.Tags.ServiceID, l.Tags.DeploymentID, l.Tags.DeploymentInstanceID),
		Time:           l.Timestamp,
		SeverityNumber: num,
		SeverityText:   text,
		Body:           l.Message,
		Attributes:     attrs,
	}
}
