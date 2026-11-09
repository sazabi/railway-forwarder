# Railway template

The one-click template is composed by hand in Railway's template composer, in a Sazabi-owned Railway workspace. This file is the source of truth for what to enter. Update it together with the composer whenever the template changes.

## Service

| Field | Value |
|-------|-------|
| Service name | `sazabi-forwarder` |
| Source | Docker image `ghcr.io/sazabi/railway-forwarder:vX.Y.Z` (the latest release; never `latest`) |
| Public networking | Off |
| Volume | None |
| Replicas | 1 |
| Restart policy | Always |

Railway can only pull the image once the GHCR package is public, so the image must be made public before the template is published.

## Variables

| Variable | Value | Template setting | Description shown to the customer |
|----------|-------|------------------|-----------------------------------|
| `SAZABI_INTAKE_URL` | empty | Required | The intake URL of your Railway log source in Sazabi (Log sources → Railway). |
| `RAILWAY_API_TOKEN` | empty | Required | A Railway account token from railway.com/account/tokens with Workspace left unset. It can read every project in your Railway account. Project and workspace tokens do not work. |
| `RAILWAY_ENVIRONMENT_ID` | `${{RAILWAY_ENVIRONMENT_ID}}` | Prefilled, hidden | The environment whose logs are forwarded. |

## Listing

**Name:** Sazabi Log Forwarder

**Short description:** Send the deploy and HTTP logs of every service in your environment to Sazabi, with no code changes.

**Category:** Observability

**Overview:**

Railway has no log drain. The Sazabi Log Forwarder runs as one extra service in your environment, reads your services' deploy logs and HTTP logs from Railway's API, and sends them to Sazabi over OpenTelemetry (OTLP/HTTP).

- Every service in the environment, including services you add later
- Deploy logs with Railway's log levels and structured attributes
- HTTP logs for services with a public domain, with 5xx as errors and 4xx as warnings
- Service, environment, project, and deployment attributes on every log
- No public networking and no volume

You need a Sazabi Railway log source for the intake URL and a Railway account token. An account token can read every project in your Railway account; the forwarder only reads its own environment. Logs only: for traces, add the OpenTelemetry SDK to your app.

Setup guide: https://docs.sazabi.com/catalogs/log-sources/send-to-an-endpoint/railway
Source: https://github.com/sazabi/railway-forwarder
