1️⃣ Clean system logs (safe + high ROI)
Check log size
sudo du -sh /var/log/*

Vacuum old systemd logs
sudo journalctl --disk-usage
sudo journalctl --vacuum-time=7d


What this does

Deletes system logs older than 7 days

Frees root disk

Safe on production servers

(Optional stricter)

sudo journalctl --vacuum-size=200M

2️⃣ Truncate Docker logs (VERY important)

Docker logs are a silent disk killer.

Find big containers
sudo du -sh /var/lib/docker/containers/*/*-json.log

Truncate all Docker logs (safe)
sudo find /var/lib/docker/containers/ -name "*-json.log" -exec truncate -s 0 {} \;


What this does

Keeps containers running

Resets log files to 0 bytes

No downtime

3️⃣ Docker cleanup (images, layers, stopped containers)
See Docker usage
docker system df

Remove unused containers, images, networks
docker system prune -f

More aggressive (optional)
docker system prune -a -f


What this does

Removes:

stopped containers

unused images

dangling volumes

Does NOT touch running containers

4️⃣ APT package cache cleanup

Ubuntu keeps old packages around.

sudo apt autoremove -y
sudo apt autoclean


What this does

Removes unused dependencies

Clears old .deb packages

Frees root disk

5️⃣ ClickHouse-specific cleanup (CRITICAL)
Check ClickHouse disk usage
sudo du -sh /mnt/clickhouse/*

Remove old ClickHouse logs
sudo du -sh /var/log/clickhouse-server
sudo truncate -s 0 /var/log/clickhouse-server/*.log


Safe — ClickHouse will continue logging.

Optional but VERY useful: TTL-based cleanup (long-term)

If you are storing raw orderbook events, you should not keep them forever on hot disk.

Example (adjust days):

ALTER TABLE orderbook_events
MODIFY TTL ts + INTERVAL 30 DAY;


What this does

Automatically deletes data older than 30 days

Prevents silent disk death

Essential for streaming systems

6️⃣ Find large files anywhere (diagnostics)
sudo du -ah / | sort -rh | head -40


What this does

Lists the 40 largest files/directories

Use this when disk grows unexpectedly

7️⃣ Set log rotation (so this never happens again)
Docker log limits (recommended)

Edit or create:

sudo nano /etc/docker/daemon.json


Add:

{
  "log-driver": "json-file",
  "log-opts": {
    "max-size": "50m",
    "max-file": "3"
  }
}


Restart Docker:

sudo systemctl restart docker

System log retention

Edit:

sudo nano /etc/systemd/journald.conf


Set:

SystemMaxUse=200M
MaxRetentionSec=7day


Restart:

sudo systemctl restart systemd-journald

8️⃣ Create a monthly “maintenance command” (copy/paste)

Save this as ~/maintenance.sh:

#!/bin/bash
set -e

echo "🧹 Cleaning system logs..."
sudo journalctl --vacuum-time=7d

echo "🐳 Truncating Docker logs..."
sudo find /var/lib/docker/containers/ -name "*-json.log" -exec truncate -s 0 {} \;

echo "🐳 Docker prune..."
docker system prune -f

echo "📦 Cleaning apt cache..."
sudo apt autoremove -y
sudo apt autoclean

echo "📊 Disk usage:"
df -h


Run monthly:

chmod +x ~/maintenance.sh
./maintenance.sh

9️⃣ What I’d recommend next (for your setup)

Given:

ClickHouse

Streaming data

Docker

EC2

You should strongly consider:

Increasing /mnt/clickhouse to 50–100G

TTL rules for all raw tables

Daily monitoring:

watch -n 60 df -h


Cold storage (Parquet → S3) for historical data