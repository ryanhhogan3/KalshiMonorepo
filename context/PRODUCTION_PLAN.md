# Production Plan: Market Making on Kalshi

## 1. Environment Setup
- Ensure Python environment and dependencies are installed.
- Configure Docker for deployment (see `infra/Dockerfile`, `docker-compose.yml`).
- Set up monitoring (Prometheus, Grafana).

## 2. Configuration
- Fill out all YAML files in `configs/` (env, routing, risk, markets).
- Set environment variables for keys, endpoints, and timeouts.

## 3. Kalshi Integration
- Implement and test all methods in `kalshi/api/client.py` for REST operations.
- Implement WebSocket connection in `kalshi/websocket/stream.py` for real-time market data.
- Map Kalshi payloads to protocol types using `kalshi/adapters/normalize.py`.
- Handle authentication, rate limiting, and error mapping.

## 4. Core Logic
- Build and test order book logic (`orderbooks/`).
- Implement feature extraction (`features/`).
- Develop and validate strategies (`controls/strategy/`).
- Set up risk management policies and engine (`controls/risk/`).

## 5. Event Logging & Analytics
- Ensure all events are logged in NDJSON/Parquet (`analytics/logger.py`).
- Set up Prometheus metrics (`analytics/metrics.py`).
- Test backtesting and reporting modules.

## 6. Orchestration Scripts
- Use scripts in `infra/scripts/` to run live trading and backtests.
- Integrate CI/CD for automated testing and deployment.

## 7. Go-Live Checklist
- Run smoke tests (`infra/scripts/smoke.py`).
- Validate risk and strategy logic in simulation.
- Monitor system health and logs.
- Deploy to production using Docker Compose.
- Start live market making using Kalshi WebSocket/API.

## 8. Post-Deployment
- Monitor metrics and logs.
- Tune models and risk parameters as needed.
- Document all changes and incidents.

---

This plan ensures a robust, auditable, and extensible market making system ready for production on Kalshi.