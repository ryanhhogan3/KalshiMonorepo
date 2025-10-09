🧱 1. Package Layers — What They Are and Where They Sit in the Business
a) Core Infrastructure Packages
Package	Purpose	Business Role
protocol/	Defines shared data schemas — how every module speaks to the others (snapshots, deltas, quotes, trades, fills, P&L).	The foundation. Makes your software auditable and institution-grade. When a firm buys this product, the schema guarantees data fidelity.
ws_ingest/	Real-time WebSocket ingestion from Kalshi’s data streams.	Your “ears on the market.” Provides ultra-low-latency signal capture — the first differentiator in execution performance.
orderbook/	Builds and maintains a synchronized view of the market (snapshots + deltas).	The “brain stem.” Without a perfect order book, your pricing is garbage. Institutions care about sequencing, correctness, and staleness detection.
features/	Transforms book data into features: spread, microprice, volatility, imbalance, and event timing.	The “analytics factory.” Converts raw feed data into tradable signals. Critical IP.


⚙️ 2. Pricing Engines — The Profit-Making Core

You’ll likely deploy three to four distinct pricing engines, each tuned for specific market conditions.

M0 – Mid-Edge (Baseline Maker)

Behavior: Quotes around mid ± fixed edge (e.g., ±1¢).

Strength: Predictable spread capture; simple inventory management.

Weakness: Vulnerable during volatile regimes.

Use: Default mode during calm markets; your “heartbeat.”

M1 – Microprice / Imbalance Model

Behavior: Adjusts bid/ask edges based on order-flow imbalance and microprice drift.

Strength: Adapts to flow direction; better at avoiding adverse selection.

Weakness: Requires accurate, low-latency book data.

Use: Main driver in active markets; a technical selling point (shows sophistication).

M2 – Regime-Aware Model

Behavior: Detects regimes (calm / active / event) via spread width and volatility.

Strength: Dynamically switches edge width, quote size, and TTL.

Weakness: Complexity; needs calibration.

Use: Handles volatility spikes and economic event windows.

M3 – Cross-Contract Arbitrage / Statistical

Behavior: Looks at correlated contracts (YES/NO or CPI vs. FedRate) and trades the spread.

Strength: Low risk; leverages correlation inefficiencies.

Weakness: Requires multi-market data feed and sync.

Use: Diversification layer — allows the system to profit even when single-market volume is low.


🧮 3. Risk Engines — The Capital Preservation Layer

Each risk engine enforces different safety policies.

R0 – Inventory Risk Engine

Tracks per-market and global inventory exposure.

Blocks quotes that push you past caps.

Implements auto-flatten when limits breach.

Why it matters: Prevents imbalance risk (holding too many YES or NO contracts).

R1 – Drawdown & Loss Limit Engine

Monitors daily realized/unrealized P&L.

Triggers “cool-down” or halts trading when limits hit.

Why it matters: Institutions must prove they can’t blow up.

R2 – Regime-Adaptive Risk Engine

Dynamically adjusts risk thresholds:

Tightens inventory limits during events.

Expands slightly during calm regimes.

Why it matters: Balances opportunity vs. protection automatically.

R3 – Portfolio Correlation Risk

Calculates aggregate exposure across correlated markets.

Caps total variance, not just raw size.

Why it matters: Lets you scale across 20+ contracts without concentration risk.