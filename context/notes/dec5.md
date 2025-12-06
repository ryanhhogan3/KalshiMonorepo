3. Create a tiny deploy script (optional but nice)

In the repo root, something like deploy.sh:

#!/usr/bin/env bash
set -euo pipefail

echo "[DEPLOY] Pulling latest code..."
git pull --rebase

echo "[DEPLOY] Rebuilding streamer and restarting stack..."
docker compose down
docker compose build streamer
docker compose up -d

echo "[DEPLOY] Status:"
docker ps


Then:

chmod +x deploy.sh


Next time you change code on GitHub, you can just SSH in and:

./deploy.sh