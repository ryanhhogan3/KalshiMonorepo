## Connect with the EC2 instance via SSH
ssh -i C:\Users\ryanh\Desktop\Code\AWS\Secrets\kalshiMonoEC2key.pem ubuntu@18.219.222.79

# Once inside EC2
# Reset docker container build
docker compose down
docker compose up -d --build

# Reset docker instance
docker restart

## Logging files and info
# view streamer logs
docker logs -f kalshi_streamer 

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