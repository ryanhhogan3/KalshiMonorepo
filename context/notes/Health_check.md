Good — your decision heartbeat is live (last_decision_ts basically “now”), and you’re producing order_actions every ~30s. So the engine is running and writing to CH correctly.
Now let’s turn this into a repeatable health check that verifies the exact behaviors you care about:


Paired maker quoting: YES/BID and NO/BID should exist (or at least be attempted) per market when risk allows.


Risk-block cancellation sweep: when risk blocks, working orders get canceled and/or orphans swept.


No silent failures: responses/rejects tracked and visible.


Below is a tight “health check pack” (ClickHouse queries + a couple log greps).

0) Set vars once
export MARKETS_SQL="'KXHIGHAUS-26JAN25-B28.5','KXHIGHAUS-26JAN25-B30.5'"
export LOOKBACK_MIN=30


1) Engine heartbeat + “am I allowed to quote?”
This tells you per market: last decision time, whether risk allowed, and current target sizes/prices.
chq "SELECT
  market_ticker,
  max(ts) AS last_ts,
  anyHeavy(engine_instance_id) AS engine,
  dateDiff('second', max(ts), now()) AS age_sec,
  anyLast(allowed) AS allowed_now,
  anyLast(block_codes) AS block_codes_now,
  anyLast(target_bid_px) AS target_yes_bid_px,
  anyLast(target_bid_sz) AS target_yes_bid_sz,
  anyLast(target_ask_px) AS target_no_ask_px,
  anyLast(target_ask_sz) AS target_no_ask_sz,
  anyLast(open_orders_count) AS open_orders_count,
  anyLast(open_yes_bid_size) AS open_yes_bid_size,
  anyLast(open_no_ask_size) AS open_no_ask_size,
  anyLast(pos_filled) AS pos_filled,
  anyLast(pos_open_exposure) AS pos_open_exposure,
  anyLast(pos_total_est) AS pos_total_est
FROM kalshi.mm_decisions
WHERE market_ticker IN (${MARKETS_SQL})
  AND ts > now() - INTERVAL ${LOOKBACK_MIN} MINUTE
GROUP BY market_ticker
ORDER BY market_ticker"

What you want to see


allowed_now = 1 when you expect quoting.


If allowed_now = 0, block_codes_now should explain it (e.g. would_exceed_cap) and then cancels should happen (see #5).



2) “Did we actually place BOTH legs?”
This is the core paired-maker check. It summarizes last 30 minutes by market for:


YES/BID places/cancels


NO/BID places/cancels  ✅ (this is what you were missing)


NO/ASK places/cancels (if you still have ask-side behavior somewhere)


chq "SELECT
  market_ticker,
  countIf(action_type='PLACE'  AND api_side='yes' AND side='BID') AS yes_bid_place,
  countIf(action_type='CANCEL' AND api_side='yes' AND side='BID') AS yes_bid_cancel,
  countIf(action_type='PLACE'  AND api_side='no'  AND side='BID') AS no_bid_place,
  countIf(action_type='CANCEL' AND api_side='no'  AND side='BID') AS no_bid_cancel,
  countIf(action_type='PLACE'  AND api_side='no'  AND side='ASK') AS no_ask_place,
  countIf(action_type='CANCEL' AND api_side='no'  AND side='ASK') AS no_ask_cancel,
  max(ts) AS last_action_ts,
  anyHeavy(engine_instance_id) AS engine
FROM kalshi.mm_order_actions
WHERE market_ticker IN (${MARKETS_SQL})
  AND ts > now() - INTERVAL ${LOOKBACK_MIN} MINUTE
GROUP BY market_ticker
ORDER BY market_ticker"

Interpretation


If yes_bid_place > 0 but no_bid_place = 0 while allowed_now = 1, your paired-quote logic still isn’t executing the NO leg (or it’s failing before action logging).



3) “Do we have a recent YES/BID that doesn’t have a matching NO/BID attempt?”
This catches “orphan leg” behavior by engine cycle time. It’s a blunt but effective detector.
chq "WITH w AS (
  SELECT
    market_ticker,
    toStartOfMinute(ts) AS m,
    countIf(action_type='PLACE' AND api_side='yes' AND side='BID') AS yes_bid_place,
    countIf(action_type='PLACE' AND api_side='no'  AND side='BID') AS no_bid_place
  FROM kalshi.mm_order_actions
  WHERE market_ticker IN (${MARKETS_SQL})
    AND ts > now() - INTERVAL ${LOOKBACK_MIN} MINUTE
  GROUP BY market_ticker, m
)
SELECT *
FROM w
WHERE yes_bid_place > 0 AND no_bid_place = 0
ORDER BY m DESC, market_ticker"

If this returns rows during allowed quoting → you are placing YES without even attempting NO.

4) “Are NO orders getting rejected silently?”
If your order gateway logs actions but responses show rejects, this surfaces it.
chq "SELECT
  market_ticker,
  status,
  count() AS n,
  anyHeavy(reject_reason) AS sample_reject
FROM kalshi.mm_order_responses
WHERE market_ticker IN (${MARKETS_SQL})
  AND ts > now() - INTERVAL ${LOOKBACK_MIN} MINUTE
GROUP BY market_ticker, status
ORDER BY market_ticker, n DESC"

Then, if you see rejects, zoom in to NO/BID only:
chq "SELECT
  a.market_ticker,
  a.ts AS action_ts,
  a.action_type,
  a.api_side,
  a.side,
  a.price_cents,
  a.size,
  r.ts AS resp_ts,
  r.status,
  r.reject_reason
FROM kalshi.mm_order_actions a
LEFT JOIN kalshi.mm_order_responses r
  ON r.client_order_id = a.client_order_id
WHERE a.market_ticker IN (${MARKETS_SQL})
  AND a.ts > now() - INTERVAL ${LOOKBACK_MIN} MINUTE
  AND a.api_side='no' AND a.side='BID'
ORDER BY a.ts DESC
LIMIT 200"

Key: if you see actions but no responses, you have a response ingestion or join-key issue; if you see rejects, it’s a trading-rule/params issue.

5) Risk-cap behavior: “did we cancel when blocked?”
You added cancel-sweeps when risk is blocked. This verifies it happened in practice.
First: check for cancels (any side) when decisions show blocked.
chq "WITH blocked AS (
  SELECT
    market_ticker,
    toStartOfMinute(ts) AS m,
    countIf(allowed=0) AS n_blocked
  FROM kalshi.mm_decisions
  WHERE market_ticker IN (${MARKETS_SQL})
    AND ts > now() - INTERVAL ${LOOKBACK_MIN} MINUTE
  GROUP BY market_ticker, m
),
cxl AS (
  SELECT
    market_ticker,
    toStartOfMinute(ts) AS m,
    countIf(action_type='CANCEL') AS n_cancel
  FROM kalshi.mm_order_actions
  WHERE market_ticker IN (${MARKETS_SQL})
    AND ts > now() - INTERVAL ${LOOKBACK_MIN} MINUTE
  GROUP BY market_ticker, m
)
SELECT
  b.market_ticker, b.m, b.n_blocked, ifNull(c.n_cancel, 0) AS n_cancel
FROM blocked b
LEFT JOIN cxl c USING (market_ticker, m)
WHERE b.n_blocked > 0
ORDER BY b.m DESC, b.market_ticker"

Expectation


Minutes with n_blocked > 0 should often have n_cancel > 0 if you had working orders.



6) “What orders are actually resting right now?” (from your own bookkeeping)
Your mm_decisions.open_yes_bid_size/open_no_* fields are only as good as your internal tracking. Still, it’s a great sanity check:
chq "SELECT
  market_ticker,
  anyLast(open_yes_bid_size) AS open_yes_bid_size,
  anyLast(open_no_ask_size) AS open_no_ask_size,
  anyLast(open_orders_count) AS open_orders_count,
  anyLast(allowed) AS allowed_now,
  anyLast(block_codes) AS block_codes_now,
  max(ts) AS last_ts
FROM kalshi.mm_decisions
WHERE market_ticker IN (${MARKETS_SQL})
  AND ts > now() - INTERVAL 5 MINUTE
GROUP BY market_ticker
ORDER BY market_ticker"

If your new logic is “YES/BID + NO/BID”, you should consider adding an open_no_bid_size field too — right now you’re tracking NO/ASK in decisions (at least in the columns you showed earlier).

Log checks (fast)
These are the “did code path execute?” proofs. Run them against your mm engine container logs:
A) Did we attempt the NO leg?
docker logs --since 30m kalshi_mm_engine 2>&1 | egrep -i "quote_place_no_bid|api_side.*no.*BID|no_bid" | tail -n 200

B) Did cancel sweeps fire when blocked?
docker logs --since 30m kalshi_mm_engine 2>&1 | egrep -i "risk_block_cancel_sweep|open_order_orphan_detected|open_order_sync_summary" | tail -n 200

(Replace container name if yours differs.)

One important note about your “pairing” goal
Having a BUY YES resting and a BUY NO resting at the same time is not a hedge of your filled YES inventory — it’s two-sided liquidity provision (you’re bidding on both outcomes). That’s fine (and common for MM), but risk math must treat them as additive potential exposure unless you enforce a shared cap.
So in the health checks above, always interpret “NO/BID present” as “we’re quoting both sides,” not “we’re neutral.”

Next step
Run Query #2 + #4 and paste outputs.


If no_bid_place = 0 but allowed_now = 1, we go straight to the code path that decides whether to generate/place NO/BID (it’s not reaching action logging).


If no_bid_place > 0 but responses are rejects, we look at reject_reason and fix order params/pricing/permissions.


If no_bid_place > 0 and responses OK but you still don’t see it on the UI, we’re in “resting vs immediately filled/canceled/replaced” territory and we’ll check lifetimes + open-order reconciliation.


If you want, I can also give you a single “one-shot dashboard query” that outputs a PASS/FAIL per market for each invariant (heartbeat, allowed, paired legs, no rejects, cancel sweep on block).