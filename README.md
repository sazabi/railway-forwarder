# Sazabi Railway log forwarder

Send the logs of every service in a [Railway](https://railway.com) environment to [Sazabi](https://sazabi.com) with no code changes.

Railway has no log drain. This forwarder runs as one extra service in your environment, reads your services' **deploy logs** and **HTTP logs** from Railway's API, and sends them to Sazabi over OTLP/HTTP. Services you add later are picked up automatically.

[![Deploy on Railway](https://railway.com/button.svg)](https://railway.com/deploy/sazabi-log-forwarder)

## Before you deploy

> [!WARNING]
> Railway's log API only accepts **account tokens**, and an account token can read **every project in your Railway account**, not just the one the forwarder runs in. The forwarder only reads logs from its own environment, but the token itself is not scoped. Store it only in this service's variables and revoke it at [railway.com/account/tokens](https://railway.com/account/tokens) if you remove the forwarder. Project and workspace tokens do not work.

You need:

- A Sazabi **Railway** log source. Its intake URL is your `SAZABI_INTAKE_URL`.
- A Railway account token from [railway.com/account/tokens](https://railway.com/account/tokens). Leave **Workspace** unset so Railway creates an account token.

## Deploy

1. Click **Deploy on Railway** and choose the project and environment whose logs you want.
2. Fill in the two variables:

   | Variable | Value |
   |----------|-------|
   | `SAZABI_INTAKE_URL` | The intake URL from your Sazabi Railway log source |
   | `RAILWAY_API_TOKEN` | Your Railway account token |

3. Deploy. Within a minute the forwarder logs `forwarding railway logs` with your project and environment, and logs start appearing in Sazabi.

Deploy one forwarder per environment you want to send (for example, one in `production` and one in `staging`).

## What gets sent

| Log | Source | Severity |
|-----|--------|----------|
| Deploy logs | Everything your services write to stdout/stderr, including Railway's structured-log attributes | Railway's level (`debug`, `info`, `warn`, `error`, ...) |
| HTTP logs | Railway's edge request log for every service with a public domain | `ERROR` for 5xx, `WARN` for 4xx, `INFO` otherwise |

Every log carries these resource attributes:

- `service.name`: the Railway service name
- `deployment.environment.name`: the Railway environment name
- `railway.project.name`, `railway.project.id`, `railway.environment.id`, `railway.service.id`, `railway.deployment.id`, and `railway.deployment.instance.id` (deploy logs)

Records also carry `railway.log.type` (`deploy` or `http`). HTTP records add `http.request.method`, `url.path`, `http.response.status_code`, `duration_ms`, `server.address`, `client.address`, `user_agent.original`, and `railway.request.id`.

Logs written before the forwarder started are not backfilled. Build logs, the forwarder's own logs, and Railway's container start/stop lines are not sent.

### Traces and metrics

This forwarder sends **logs only**; Railway's API does not expose traces. For traces, instrument your app with the OpenTelemetry SDK and point it at the same Sazabi intake URL. See the [Sazabi Railway docs](https://docs.sazabi.com/catalogs/log-sources/send-to-an-endpoint/railway).

## Optional settings

| Variable | Default | Purpose |
|----------|---------|---------|
| `ENABLE_DEPLOY_LOGS` | `true` | Send deploy logs |
| `ENABLE_HTTP_LOGS` | `true` | Send HTTP logs |
| `REFRESH_INTERVAL` | `30s` | How often to look for new services and deployments |
| `LOG_LEVEL` | `info` | The forwarder's own log level |

`RAILWAY_ENVIRONMENT_ID` and `RAILWAY_SERVICE_ID` are provided by Railway; you do not set them.

## Resources

The forwarder needs no public networking and no volume, and runs as a single replica.

## Troubleshooting

- **`Not Authorized`**: `RAILWAY_API_TOKEN` is not an account token, or was revoked.
- **`intake rejected batch: status 401`/`403`/`404`**: `SAZABI_INTAKE_URL` is wrong. Copy it again from the Sazabi log source.
- **No HTTP logs for a service**: only services with a Railway or custom domain have HTTP logs.

## Development

See [CLAUDE.md](CLAUDE.md). This repository is a release mirror of `packages/railway-forwarder` in the Sazabi monorepo; a maintainer-dispatched release pushes a `vX.Y.Z` tag here, which publishes `ghcr.io/sazabi/railway-forwarder:vX.Y.Z`.

## Credits

Adapts the Railway API approach of [Locomotive](https://github.com/brody192/locomotive) (MIT). See [THIRD_PARTY_NOTICES](THIRD_PARTY_NOTICES).

## License

[Apache 2.0](LICENSE)
