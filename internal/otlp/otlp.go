// Package otlp builds OTLP/HTTP JSON log payloads and exports them in batches.
package otlp

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"
)

// Resource identifies where a log came from in Railway.
type Resource struct {
	ServiceName          string
	EnvironmentName      string
	ProjectName          string
	ProjectID            string
	EnvironmentID        string
	ServiceID            string
	DeploymentID         string
	DeploymentInstanceID string
}

func (r Resource) attributes() []KeyValue {
	attrs := make([]KeyValue, 0, 8)
	add := func(k, v string) {
		if v != "" {
			attrs = append(attrs, String(k, v))
		}
	}
	add("service.name", r.ServiceName)
	add("deployment.environment.name", r.EnvironmentName)
	add("railway.project.name", r.ProjectName)
	add("railway.project.id", r.ProjectID)
	add("railway.environment.id", r.EnvironmentID)
	add("railway.service.id", r.ServiceID)
	add("railway.deployment.id", r.DeploymentID)
	add("railway.deployment.instance.id", r.DeploymentInstanceID)
	return attrs
}

// Record is one log record bound for Sazabi.
type Record struct {
	Resource       Resource
	Time           time.Time
	SeverityNumber int
	SeverityText   string
	Body           string
	Attributes     []KeyValue
}

type KeyValue struct {
	Key   string   `json:"key"`
	Value AnyValue `json:"value"`
}

type AnyValue struct {
	StringValue *string  `json:"stringValue,omitempty"`
	BoolValue   *bool    `json:"boolValue,omitempty"`
	IntValue    *string  `json:"intValue,omitempty"`
	DoubleValue *float64 `json:"doubleValue,omitempty"`
}

func String(k, v string) KeyValue { return KeyValue{Key: k, Value: AnyValue{StringValue: &v}} }

func Int(k string, v int64) KeyValue {
	s := strconv.FormatInt(v, 10)
	return KeyValue{Key: k, Value: AnyValue{IntValue: &s}}
}

func Double(k string, v float64) KeyValue { return KeyValue{Key: k, Value: AnyValue{DoubleValue: &v}} }

func Bool(k string, v bool) KeyValue { return KeyValue{Key: k, Value: AnyValue{BoolValue: &v}} }

// FromJSON converts a JSON-encoded value into an attribute. Objects and arrays are
// kept as their JSON text; values that are not valid JSON are kept verbatim.
func FromJSON(k, raw string) KeyValue {
	var v any
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil || dec.More() {
		return String(k, raw)
	}
	switch t := v.(type) {
	case string:
		return String(k, t)
	case bool:
		return Bool(k, t)
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return Int(k, i)
		}
		if f, err := t.Float64(); err == nil {
			return Double(k, f)
		}
		return String(k, t.String())
	case nil:
		return String(k, "")
	default:
		return String(k, raw)
	}
}

// Severity maps a log level name to its OTLP severity number and canonical text.
func Severity(level string) (int, string) {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "trace":
		return 1, "TRACE"
	case "debug":
		return 5, "DEBUG"
	case "warn", "warning":
		return 13, "WARN"
	case "error", "err":
		return 17, "ERROR"
	case "fatal", "critical", "crit", "panic", "emergency", "emerg", "alert":
		return 21, "FATAL"
	default:
		return 9, "INFO"
	}
}

type exportRequest struct {
	ResourceLogs []resourceLogs `json:"resourceLogs"`
}

type resourceLogs struct {
	Resource  resource    `json:"resource"`
	ScopeLogs []scopeLogs `json:"scopeLogs"`
}

type resource struct {
	Attributes []KeyValue `json:"attributes"`
}

type scopeLogs struct {
	Scope      scope       `json:"scope"`
	LogRecords []logRecord `json:"logRecords"`
}

type scope struct {
	Name    string `json:"name"`
	Version string `json:"version,omitempty"`
}

type logRecord struct {
	TimeUnixNano         string     `json:"timeUnixNano"`
	ObservedTimeUnixNano string     `json:"observedTimeUnixNano"`
	SeverityNumber       int        `json:"severityNumber"`
	SeverityText         string     `json:"severityText"`
	Body                 AnyValue   `json:"body"`
	Attributes           []KeyValue `json:"attributes,omitempty"`
}

const scopeName = "github.com/sazabi/railway-forwarder"

// Encode groups records by resource into one OTLP JSON export request.
func Encode(records []Record, version string, observed time.Time) ([]byte, error) {
	index := make(map[Resource]int)
	req := exportRequest{}
	obs := strconv.FormatInt(observed.UnixNano(), 10)
	for _, r := range records {
		i, ok := index[r.Resource]
		if !ok {
			i = len(req.ResourceLogs)
			index[r.Resource] = i
			req.ResourceLogs = append(req.ResourceLogs, resourceLogs{
				Resource:  resource{Attributes: r.Resource.attributes()},
				ScopeLogs: []scopeLogs{{Scope: scope{Name: scopeName, Version: version}}},
			})
		}
		body := r.Body
		sl := &req.ResourceLogs[i].ScopeLogs[0]
		sl.LogRecords = append(sl.LogRecords, logRecord{
			TimeUnixNano:         strconv.FormatInt(r.Time.UnixNano(), 10),
			ObservedTimeUnixNano: obs,
			SeverityNumber:       r.SeverityNumber,
			SeverityText:         r.SeverityText,
			Body:                 AnyValue{StringValue: &body},
			Attributes:           r.Attributes,
		})
	}
	return json.Marshal(req)
}
