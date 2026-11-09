package config

import (
	"strings"
	"testing"
	"time"
)

func env(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func TestLoadDefaults(t *testing.T) {
	c, err := Load(env(map[string]string{
		"SAZABI_INTAKE_URL":      "https://intake.example.com/abc",
		"RAILWAY_API_TOKEN":      "tok",
		"RAILWAY_ENVIRONMENT_ID": "env",
		"RAILWAY_SERVICE_ID":     "self",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if !c.DeployLogs || !c.HTTPLogs || c.RefreshInterval != 30*time.Second || c.SelfServiceID != "self" {
		t.Errorf("unexpected config %+v", c)
	}
	if c.RailwayWSURL != "wss://backboard.railway.com/graphql/v2" {
		t.Errorf("ws url = %s", c.RailwayWSURL)
	}
}

func TestLoadReportsEveryMissingInput(t *testing.T) {
	_, err := Load(env(nil))
	if err == nil {
		t.Fatal("expected error")
	}
	for _, want := range []string{"SAZABI_INTAKE_URL", "RAILWAY_API_TOKEN", "RAILWAY_ENVIRONMENT_ID"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %s", err, want)
		}
	}
}

func TestLoadRejectsBadValues(t *testing.T) {
	_, err := Load(env(map[string]string{
		"SAZABI_INTAKE_URL":      "intake.example.com",
		"RAILWAY_API_TOKEN":      "tok",
		"RAILWAY_ENVIRONMENT_ID": "env",
		"ENABLE_HTTP_LOGS":       "maybe",
		"REFRESH_INTERVAL":       "1s",
	}))
	if err == nil {
		t.Fatal("expected error")
	}
	for _, want := range []string{"http(s) URL", "ENABLE_HTTP_LOGS", "REFRESH_INTERVAL"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %s", err, want)
		}
	}
}
