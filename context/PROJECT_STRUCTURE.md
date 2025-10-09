# Project Structure Overview

## Top-Level Folders

- **protocol/**: Exchange-agnostic types & contracts. Defines wire types, intents, state, and errors. Single source of truth for types.
- **orderbooks/**: Pure order book builder and invariants. Deterministic, testable, used live and in backtests.
- **features/**: Converts order book data into model-ready features and rolling stats.
- **controls/**: Contains strategy and risk logic. Strategies are exchange-agnostic and only interact with protocol types.
- **analytics/**: Logging, metrics, backtesting, and reporting. Provides audit trail and P&L attribution.
- **configs/**: YAML configuration for environment, routing, risk, and markets. Controls behavior via config, not code.
- **infra/**: Infrastructure and ops. Docker, CI, monitoring, scripts for running live and backtests.
- **kalshi/**: All Kalshi-specific code (API, WebSocket, adapters). Isolated from core logic for easy venue extension.

## Interactions

- **protocol/** types are imported everywhere for consistency.
- **orderbooks/** builds and maintains order book state, used by features and analytics.
- **features/** computes signals from order books for strategies.
- **controls/strategy/** reads features and market state, emits intents (QuoteIntent, CancelIntent).
- **controls/risk/** validates intents before orders are sent.
- **kalshi/adapters/** bridges Kalshi API/WebSocket payloads to protocol types.
- **kalshi/api/** and **kalshi/websocket/** handle all communication with Kalshi, isolated from core logic.
- **analytics/** logs all events, runs backtests, and generates reports.
- **infra/** scripts orchestrate live trading and backtesting, using configs and core modules.

## Data Flow Example
1. **WebSocket/API** (kalshi) receives market data →
2. **Adapters** normalize to protocol events →
3. **OrderBook** applies events, updates state →
4. **Features** compute signals →
5. **Strategy** selects model, emits intents →
6. **RiskEngine** validates intents →
7. **API** sends orders to Kalshi →
8. **Analytics** logs all actions/events.

---

This modular structure ensures clear separation of concerns, easy extensibility, and robust auditability.