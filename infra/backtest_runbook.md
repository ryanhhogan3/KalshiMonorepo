## Backtest Runbook

This runbook describes how to run the backtest runner locally (via Docker Compose) and on an EC2 host.

Assumptions
- You have the repo checked out on the host.
- `docker` and `docker-compose` are installed.
- ClickHouse service is defined in `docker-compose.yml` (this repo includes one).

Local Docker Compose (quick)
1. Build and start ClickHouse + streamer (if needed):

Run the following from the repo root:

docker-compose up -d clickhouse

2. Start the backtest runner container (it will sit idle):

docker-compose up -d backtest_runner

3. Generate the parquet fixture (inside the service or on host). From host run:

python -m backtest.cli generate-fixture

4. Run a smoke backtest inside the `backtest_runner` container:

docker-compose run --rm backtest_runner python -m backtest.cli run --market SAMPLE.MKT --start 2025-11-01T10:00:00Z --end 2025-11-01T10:01:00Z --source parquet --fixture /app/tests/fixtures/sample_market_1min.parquet

5. Examine outputs in `runs/` on host (mounted by the container):

ls runs/run_SAMPLE.MKT_2025-11-01T10:00:00Z_2025-11-01T10:01:00Z
cat runs/run_SAMPLE.MKT_2025-11-01T10:00:00Z_2025-11-01T10:01:00Z/summary.json

EC2 (quick safe staging)
1. Copy repo to EC2 and install Docker (or use an AMI with Docker pre-installed).
2. Place your demo/private keys under /home/ubuntu/.kalshi/demo_keys.pem (ensure file perms are secure) and set .env variables for DEMO_KEYFILE path.
3. Start ClickHouse and backtest runner on EC2:

docker-compose up -d clickhouse
docker-compose up -d backtest_runner

4. Run a safety-first fixture run (no live API keys required):

docker-compose run --rm backtest_runner python -m backtest.cli generate-fixture
docker-compose run --rm backtest_runner python -m backtest.cli run --market SAMPLE.MKT --start 2025-11-01T10:00:00Z --end 2025-11-01T10:01:00Z --source parquet --fixture /app/tests/fixtures/sample_market_1min.parquet

5. Tail logs and verify outputs:

docker logs -f backtest_runner
ls runs

Safety notes
- Use demo API keys and small sizes during staging.
- The `backtest_runner` is inert by default; use `docker-compose run` to execute jobs.
- If you need a persistent cron/runner, replace `command` in `docker-compose.yml` with the desired invocation.

Troubleshooting
- If ClickHouse is not reachable, ensure `CH_URL` env or `CLICKHOUSE_URL` are set correctly.
- If fixtures do not appear, re-run `generate-fixture` inside the container and confirm `/app/tests/fixtures` is mounted.
