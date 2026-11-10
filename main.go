// Command railway-forwarder streams a Railway environment's deploy and HTTP logs to
// Sazabi over OTLP/HTTP.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/sazabi/railway-forwarder/internal/config"
	"github.com/sazabi/railway-forwarder/internal/forwarder"
	"github.com/sazabi/railway-forwarder/internal/otlp"
	"github.com/sazabi/railway-forwarder/internal/railway"
)

// version is set at build time with -ldflags "-X main.version=vX.Y.Z".
var version = "dev"

func main() {
	if err := run(); err != nil {
		slog.Error("railway-forwarder exited", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load(os.Getenv)
	if err != nil {
		return fmt.Errorf("invalid configuration:\n%w", err)
	}

	var level slog.Level
	levelErr := level.UnmarshalText([]byte(cfg.LogLevel))
	if levelErr != nil {
		level = slog.LevelInfo
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(log)
	log.Info("starting railway-forwarder", "version", version)
	if levelErr != nil {
		log.Warn("invalid log level, using info", "error", levelErr, "log_level", cfg.LogLevel)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	exporter := otlp.NewExporter(cfg.IntakeURL, version, log)
	exportCtx, stopExport := context.WithCancel(context.Background())
	go exporter.Run(exportCtx)

	rw := railway.NewClient(railway.ClientOptions{
		Token:   cfg.RailwayToken,
		HTTPURL: cfg.RailwayHTTPURL,
		WSURL:   cfg.RailwayWSURL,
	})
	fw := forwarder.New(rw, exporter, forwarder.Options{
		EnvironmentID:   cfg.EnvironmentID,
		SelfServiceID:   cfg.SelfServiceID,
		DeployLogs:      cfg.DeployLogs,
		HTTPLogs:        cfg.HTTPLogs,
		RefreshInterval: cfg.RefreshInterval,
	}, log)

	runErr := fw.Run(ctx)
	stopExport()
	exporter.Wait()
	if runErr != nil {
		if railway.IsNotAuthorized(runErr) {
			return fmt.Errorf("%w (RAILWAY_API_TOKEN must be an account token; project and workspace tokens cannot read logs)", runErr)
		}
		return runErr
	}
	log.Info("railway-forwarder stopped")
	return nil
}
