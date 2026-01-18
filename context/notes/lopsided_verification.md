C) Verification + kill criteria (1–2 days, then done)
Day 1: Functional verification (in prod/sandbox)

Run these checks every 15–30 minutes for 3 markets:

Two-sided actions present

SELECT market_ticker, side, sum(action_type='PLACE') places, sum(action_type='CANCEL') cancels
FROM kalshi.mm_order_actions
WHERE ts > now() - INTERVAL 30 MINUTE
  AND market_ticker IN (...)
GROUP BY market_ticker, side;


Pass: both BID and ASK present (unless risk blocks, then logs show it).

Cancel not_found rate

SELECT market_ticker, count() n
FROM kalshi.mm_order_responses
WHERE ts > now() - INTERVAL 30 MINUTE
  AND reject_reason ILIKE '%not_found%'
GROUP BY market_ticker;


Pass: near zero; occasional is OK but should not cluster.

Churn ceiling
Set a hard SLA: e.g. <= 4 replaces/min/side/market (tune).

SELECT market_ticker, side, toStartOfMinute(ts) m,
       sum(action_type='PLACE') places, sum(action_type='CANCEL') cancels
FROM kalshi.mm_order_actions
WHERE ts > now() - INTERVAL 60 MINUTE
  AND market_ticker IN (...)
GROUP BY market_ticker, side, m;


Pass: stable, no 14–15/min spam.

Fill symmetry / inventory mean reversion

SELECT market_ticker, side, count() n, sum(size) sz
FROM kalshi.mm_fills
WHERE ts > now() - INTERVAL 6 HOUR
  AND market_ticker IN (...)
GROUP BY market_ticker, side;


Pass: if you have fills, you should see both yes and no over time, and inventory should not drift without correction.

Inventory bounds

SELECT market_ticker, argMax(position, ts) pos
FROM kalshi.mm_positions
WHERE ts > now() - INTERVAL 6 HOUR
  AND market_ticker IN (...)
GROUP BY market_ticker;


Pass: pos stays within configured risk max and tends back toward 0 if market allows.