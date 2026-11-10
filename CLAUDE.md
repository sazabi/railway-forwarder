# railway-forwarder

Railway has no log drain, so this small Go service runs inside a customer's Railway environment, reads every service's deploy and HTTP logs from Railway's GraphQL API, and sends them to Sazabi as OTLP/HTTP logs. It is distributed as the Sazabi one-click Railway template and as `ghcr.io/sazabi/railway-forwarder:vX.Y.Z`.

## About

This is a release mirror. The forwarder is developed in the private Sazabi monorepo (`packages/railway-forwarder`); each release lands here as the exact source the image is built from. Do not commit directly: content is published here by a maintainer-dispatched release run (`.github/workflows/railway-forwarder-public-repo-sync.yml` in the monorepo), and direct commits are overwritten by the next release. The public repository's `release.yml` builds `ghcr.io/sazabi/railway-forwarder:vX.Y.Z` from the tag that run pushes.

## Architecture

| Path | Role |
|------|------|
| `main.go` | Entry point: config, signal handling, wiring |
| `internal/config/` | Environment variables (`SAZABI_INTAKE_URL`, `RAILWAY_API_TOKEN`, Railway-injected IDs, optional toggles) |
| `internal/railway/` | GraphQL client: HTTP queries and `graphql-transport-ws` subscriptions, environment/project metadata |
| `internal/forwarder/` | Deploy-log stream (one unfiltered environment subscription) and HTTP-log streams (one per live deployment with a domain), service discovery, record mapping |
| `internal/otlp/` | OTLP/HTTP JSON encoding, severity mapping, batching exporter with retries |

## Development

```bash
go vet ./...
go test -race ./...
gofmt -l .
docker build --build-arg VERSION=dev -t railway-forwarder:dev .
```

Run locally against a real environment with `SAZABI_INTAKE_URL`, `RAILWAY_API_TOKEN` (an account token) and `RAILWAY_ENVIRONMENT_ID` set, then `go run .`.

## Conventions

- Customer inputs are only `SAZABI_INTAKE_URL` and `RAILWAY_API_TOKEN`. Everything else has a default or is injected by Railway; do not add required variables.
- The forwarder's own service (`RAILWAY_SERVICE_ID`) is never forwarded, or it would loop on its own logs.
- HTTP log bodies are JSON whose keys (`method`, `path`, `status`, `durationMs`, `ip`, `userAgent`) are what Sazabi's `railway` intake adapter reads; renaming them breaks request records in Sazabi.
- Never publish or reference a `latest` image tag; the template pins a version.
- Keep the Locomotive MIT notice in `THIRD_PARTY_NOTICES`.

## Issues & support

File issues here or at [support@sazabi.ai](mailto:support@sazabi.ai).
