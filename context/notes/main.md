## Connect with the EC2 instance via SSH
ssh -i C:\Users\ryanh\Desktop\Code\AWS\Secrets\kalshiMonoEC2key.pem ubuntu@18.219.222.79

ssh -i C:\Users\ryanh\Desktop\AWS\kalshiMonoEC2key.pem ubuntu@18.219.222.79

# Once inside EC2
# Reset docker container build
docker compose down
docker compose up -d --build

# UP
 docker compose -f docker-compose.yml -f docker-compose.mm.yml up -d

 docker compose -f docker-compose.yml -f docker-compose.mm.yml up -d --build --force-recreate

# DOWN
docker compose -f docker-compose.yml -f docker-compose.mm.yml down

# Bring down engine only (for MM)
docker compose -f docker-compose.yml -f docker-compose.mm.yml stop kalshi_mm_engine
docker compose -f docker-compose.yml -f docker-compose.mm.yml rm -f kalshi_mm_engine

# Reset docker instance
docker restart

## Logging files and info
# view streamer logs
docker logs -f kalshi_streamer 

# Verify ClickHouse is reachable and writing:
curl -s http://localhost:8123/ping && echo
docker exec -it clickhouse clickhouse-client --query "SELECT count() FROM kalshi.orderbook_events;"

# Rotating file logs (inside the container)
Inside the running container:
/app/logs/

Contains:
A. Session Logs (one file per run)

--Reading App Logs Under /app/logs
These logs are inside the streamer container, not ClickHouse.

# Enter the streamer container:
docker exec -it kalshi_streamer bash

Inside:
ls -lh /app/logs

# ClickHouse Server Logs (inside the clickhouse container)
These are your database engine logs.
Inspectable with:

1. docker exec -it clickhouse bash
    --> type: clickhouse
2. ls /var/log/clickhouse-server

two files stored here: clickhouse-server.err.log  clickhouse-server.log

--A. Tail the main ClickHouse log
tail -f /var/log/clickhouse-server/clickhouse-server.log

B. Tail Error only log
tail -f /var/log/clickhouse-server/clickhouse-server.err.log


## EC2 and Docker Storage Commands
# Global disk usage
df -h

# Docker-level usage
docker system df

## Github Commands
# After a fresh push from local files, use these in Ec2

# 1. Update code
git pull

# 2. Stop old containers
docker compose down

# 3. Rebuild image(s) with the new code and start
docker compose up -d --build

From inside the repo:

cd ~/apps/KalshiMonorepo

# 1) Make sure EVERYTHING is tracked (even uncommitted tweaks)
git add -A

# 2) Commit the current EC2 state
git commit -m "EC2 backup before pulling new upstream changes"


Now create a clearly-named backup branch:

# 3) Create a backup branch from this commit
git branch ec2-backup-$(date +%Y%m%d)

# 4) Push it to GitHub so it's safe even if EC2 dies
git push -u origin ec2-backup-$(date +%Y%m%d)


You now have:

origin/ec2-backup-20251212 (for example)

That branch contains exactly the EC2 code as it was before updating.

You can always restore it via:

git switch ec2-backup-20251212

2. Reset EC2 to the latest code from GitHub

Now we want EC2 to match remote main (the new code you merged and tested elsewhere).

cd ~/apps/KalshiMonorepo

# 1) Go to main branch
git switch main

# 2) Make sure we have latest from GitHub
git fetch origin

# 3) Hard reset EC2 main to *exactly* match origin/main
git reset --hard origin/main


At this point:

Your EC2 repo = exact same code as on GitHub main

Your old EC2 customizations are safely saved on ec2-backup-YYYYMMDD

Nothing in /home/ubuntu/.kalshi/prod_keys.pem or your .env is touched by git.

3. Redeploy the new code with Docker

From the repo:

cd ~/apps/KalshiMonorepo

# Stop old containers
docker compose down

# Rebuild images with new code
docker compose build

# Start everything again
docker compose up -d


Check the containers:

docker ps


Then tail the streamer logs to confirm it’s running with the new code:

docker logs -f kalshi_streamer


You should see:

The full list of markets

The new ticker_health heartbeat lines

The improved gap / resnapshot warnings


### Clickhouse Queries:
clear tables (fast, irreversible):
docker exec -it clickhouse clickhouse-client -q "
  TRUNCATE TABLE kalshi.orderbook_events;
  TRUNCATE TABLE kalshi.latest_levels;
"

### QA Queries
Option A (best) — Run QA inside the same Docker network

This avoids host networking + auth drift.

docker exec -it kalshi_streamer /bin/sh -lc 'python3 /app/src/databases/qa/qa_clickhouse.py --hours 1 --days 7'

If you want a “daily health” quick check:

docker exec -it kalshi_streamer /bin/sh -lc \
'python3 /app/src/databases/qa/qa_clickhouse.py --hours 6 --days 2'

# This reclaims the space right now:

docker exec -i clickhouse clickhouse-client <<'SQL'
TRUNCATE TABLE system.trace_log;
TRUNCATE TABLE system.text_log;
TRUNCATE TABLE system.query_log;
TRUNCATE TABLE system.processors_profile_log;
TRUNCATE TABLE system.part_log;
TRUNCATE TABLE system.metric_log;
TRUNCATE TABLE system.asynchronous_metric_log;
SYSTEM FLUSH LOGS;
SQL