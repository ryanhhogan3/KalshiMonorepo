2. Gaps / things to double-check before “prod”

Here are the main gaps I see:

Are all inserts going through _insert_with_retry?

If latest_levels or any other table path still uses _insert_compat directly, that’s a hole.

Next step: grep for .insert( and _insert_compat to make sure everything flows through the retry layer.

No explicit backpressure from “CH broken” → “WS ingestion.”

Right now, if ClickHouse is down or heavily failing:

You still happily consume WS traffic.

The sink logs insert_failures, maybe drops batches after retries.

That’s acceptable for best-effort capture, but know it means: if CH is unhealthy, you can silently lose data while the process stays “up”.

We mitigated this by:

Counters (insert_failures) and heartbeat logs you can monitor.

Healthcheck is storage-only, not streamer-loop health.

python -m kalshifolder.healthcheck proves:

Container is up.

Python env is fine.

ClickHouse reachable.

It does not prove:

WS is actually connected.

Events are flowing.

In “real prod,” you might want a second probe (or a log-based alert) that checks rows_ingested_last_5m > 0 or recent streamer_heartbeat with events_total increasing.

Logs & data volumes on EC2.

For prod you want:

ClickHouse data on a mounted EBS volume.

App logs on a volume that won’t fill root disk.

Make sure docker-compose.yml has:

./clickhouse_data:/var/lib/clickhouse

./logs:/app/logs (or wherever your logging_config writes).

Instance sizing + OS limits.

You were hitting (total) memory limit exceeded with ~7 GiB configured in CH logs.

That suggests:

Either the instance was small, or other processes + CH blew past the limit.

Before “prod”:

Pick an EC2 type that gives you headroom (e.g. 16GB+ for running both CH + app comfortably).

Make sure swap is configured (or intentionally disabled with a conscious decision).

Secrets and env handling.

Ensure:

Kalshi API keys / RSA material are set via .env or SSM, not hard-coded.

EC2 security groups only expose:

8123/9000 to IPs you trust (or only inside VPC).

SSH locked down.

Backups & retention.

ClickHouse is your hot store; you probably don’t want to lose it.

At minimum:

Nightly snapshot of the EBS volume or CH backup script to S3.

Logs:

Rotating is in place; consider shipping logs to CloudWatch or an S3 bucket for long-term retention.