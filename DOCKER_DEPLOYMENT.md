# Docker Deployment Instructions

## Building the Docker Image

```bash
# Build the image
docker build -t kalshi-streamer:latest .

# Or, build with a specific tag
docker build -t kalshi-streamer:v1.0 .
```

## Running the Container

### With Environment File

Create a `.env.prod` file with your secrets:
```bash
PROD_KEYID=your-api-key-id
PROD_KEYFILE=/app/secrets/desktop.txt
MARKET_TICKERS=FEDFUNDS-DEC25,KXBTCMAXY-25-DEC31-129999.99
CLICKHOUSE_URL=http://clickhouse-host:8123
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=your-password
CLICKHOUSE_DATABASE=kalshi
```

Run the container:
```bash
docker run --env-file .env.prod \
  -v /path/to/secrets:/app/secrets:ro \
  -v /path/to/data:/app/data \
  kalshi-streamer:latest
```

### With Docker Compose

1. Create a `.env` file in the project root with your credentials.
2. Mount secrets volume and set paths:
```bash
docker-compose up -d
```

To stop:
```bash
docker-compose down
```

To view logs:
```bash
docker-compose logs -f streamer
```

## Kubernetes Deployment

### ConfigMap for non-secret configuration
```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: kalshi-config
data:
  MARKET_TICKERS: "FEDFUNDS-DEC25,KXBTCMAXY-25-DEC31-129999.99"
  BATCH_ROWS: "2000"
  BATCH_FLUSH_SECS: "0.010"
  CLICKHOUSE_URL: "http://clickhouse-service:8123"
  CLICKHOUSE_DATABASE: "kalshi"
```

### Secret for sensitive data
```yaml
apiVersion: v1
kind: Secret
metadata:
  name: kalshi-secrets
type: Opaque
stringData:
  PROD_KEYID: "your-key-id"
  PROD_KEYFILE: "/var/secrets/desktop.txt"
  CLICKHOUSE_PASSWORD: "your-password"
```

### Pod deployment
```yaml
apiVersion: v1
kind: Pod
metadata:
  name: kalshi-streamer
spec:
  containers:
  - name: streamer
    image: kalshi-streamer:latest
    envFrom:
    - configMapRef:
        name: kalshi-config
    - secretRef:
        name: kalshi-secrets
    volumeMounts:
    - name: secrets-volume
      mountPath: /var/secrets
      readOnly: true
    - name: data-volume
      mountPath: /app/data
  volumes:
  - name: secrets-volume
    secret:
      secretName: kalshi-secrets
  - name: data-volume
    persistentVolumeClaim:
      claimName: kalshi-data-pvc
```

## Health Checks

The container runs by default:
```bash
python -m kalshifolder.websocket.streamer
```

To check if it's running:
```bash
docker ps | grep kalshi-streamer
docker logs <container-id>
```

For health checks, consider adding a monitoring endpoint or sidecar.

## Cloud Deployment (AWS ECS, GCP Cloud Run, Azure Container Instances)

### AWS ECS Task Definition (simplified)
```json
{
  "family": "kalshi-streamer",
  "image": "123456789012.dkr.ecr.us-east-1.amazonaws.com/kalshi-streamer:latest",
  "memory": 512,
  "cpu": 256,
  "environment": [
    {
      "name": "CLICKHOUSE_URL",
      "value": "http://clickhouse-host:8123"
    }
  ],
  "secrets": [
    {
      "name": "PROD_KEYID",
      "valueFrom": "arn:aws:secretsmanager:us-east-1:123456789012:secret:kalshi/key-id"
    },
    {
      "name": "PROD_KEYFILE",
      "valueFrom": "arn:aws:secretsmanager:us-east-1:123456789012:secret:kalshi/key-file"
    }
  ],
  "mountPoints": [
    {
      "sourceVolume": "data-volume",
      "containerPath": "/app/data"
    }
  ],
  "volumes": [
    {
      "name": "data-volume",
      "efsVolumeConfiguration": {
        "fileSystemId": "fs-12345678"
      }
    }
  ]
}
```

### GCP Cloud Run Deployment
```bash
gcloud run deploy kalshi-streamer \
  --image gcr.io/my-project/kalshi-streamer:latest \
  --platform managed \
  --region us-east1 \
  --memory 512Mi \
  --cpu 2 \
  --set-env-vars CLICKHOUSE_URL=http://clickhouse:8123 \
  --set-secrets PROD_KEYID=kalshi-keyid:latest \
  --set-secrets PROD_KEYFILE=kalshi-keyfile:latest
```

## Troubleshooting

### Container fails to start
```bash
docker logs <container-id>
```

### ClickHouse connection refused
- Ensure ClickHouse is running and accessible
- Check `CLICKHOUSE_URL` is correct
- Verify network connectivity: `docker exec <container> curl http://clickhouse:8123/ping`

### Out of memory
- Increase container memory limit: `docker run -m 1g ...`
- Reduce `BATCH_ROWS` or `size_check_every` in streamer

### Parquet files not being created
- Check disk space in `/app/data`
- Verify permissions: `docker exec <container> ls -la /app/data`
- Increase `max_bytes` threshold if needed
