// Package config reads the forwarder's settings from the environment.
package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/sazabi/railway-forwarder/internal/railway"
)

type Config struct {
	IntakeURL       string
	RailwayToken    string
	EnvironmentID   string
	SelfServiceID   string
	DeployLogs      bool
	HTTPLogs        bool
	RefreshInterval time.Duration
	RailwayHTTPURL  string
	RailwayWSURL    string
	LogLevel        string
}

// Load reads the configuration. Only SAZABI_INTAKE_URL and RAILWAY_API_TOKEN are
// customer inputs; Railway injects RAILWAY_ENVIRONMENT_ID and RAILWAY_SERVICE_ID.
func Load(getenv func(string) string) (Config, error) {
	if getenv == nil {
		getenv = os.Getenv
	}
	c := Config{
		IntakeURL:      strings.TrimSpace(getenv("SAZABI_INTAKE_URL")),
		RailwayToken:   strings.TrimSpace(getenv("RAILWAY_API_TOKEN")),
		EnvironmentID:  strings.TrimSpace(getenv("RAILWAY_ENVIRONMENT_ID")),
		SelfServiceID:  strings.TrimSpace(getenv("RAILWAY_SERVICE_ID")),
		RailwayHTTPURL: orDefault(getenv("RAILWAY_GRAPHQL_URL"), railway.DefaultHTTPURL),
		RailwayWSURL:   orDefault(getenv("RAILWAY_GRAPHQL_WS_URL"), railway.DefaultWSURL),
		LogLevel:       orDefault(getenv("LOG_LEVEL"), "info"),
	}
	var errs []error
	if c.IntakeURL == "" {
		errs = append(errs, errors.New("SAZABI_INTAKE_URL is required: copy the intake URL from your Railway log source in Sazabi"))
	} else if u, err := url.Parse(c.IntakeURL); err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" {
		errs = append(errs, errors.New("SAZABI_INTAKE_URL must be an http(s) URL"))
	}
	if c.RailwayToken == "" {
		errs = append(errs, errors.New("RAILWAY_API_TOKEN is required: create an account token at https://railway.com/account/tokens"))
	}
	if c.EnvironmentID == "" {
		errs = append(errs, errors.New("RAILWAY_ENVIRONMENT_ID is required: set it to ${{RAILWAY_ENVIRONMENT_ID}}"))
	}
	var err error
	if c.DeployLogs, err = boolVar(getenv, "ENABLE_DEPLOY_LOGS", true); err != nil {
		errs = append(errs, err)
	}
	if c.HTTPLogs, err = boolVar(getenv, "ENABLE_HTTP_LOGS", true); err != nil {
		errs = append(errs, err)
	}
	c.RefreshInterval = 30 * time.Second
	if v := strings.TrimSpace(getenv("REFRESH_INTERVAL")); v != "" {
		d, perr := time.ParseDuration(v)
		if perr != nil || d < 5*time.Second {
			errs = append(errs, fmt.Errorf("REFRESH_INTERVAL must be a duration of at least 5s, got %q", v))
		} else {
			c.RefreshInterval = d
		}
	}
	if !c.DeployLogs && !c.HTTPLogs && len(errs) == 0 {
		errs = append(errs, errors.New("ENABLE_DEPLOY_LOGS and ENABLE_HTTP_LOGS are both false; nothing to forward"))
	}
	return c, errors.Join(errs...)
}

func orDefault(v, def string) string {
	if v = strings.TrimSpace(v); v != "" {
		return v
	}
	return def
}

func boolVar(getenv func(string) string, key string, def bool) (bool, error) {
	v := strings.TrimSpace(getenv(key))
	if v == "" {
		return def, nil
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false, fmt.Errorf("%s must be true or false, got %q", key, v)
	}
	return b, nil
}
