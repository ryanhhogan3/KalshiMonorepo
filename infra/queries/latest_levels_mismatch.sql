-- Detection: events fresh but latest_levels stale per ticker
SELECT
  e.market_ticker,
  max(e.ts) AS events_max,
  max(l.ingest_ts) AS levels_ingest_max,
  dateDiff('second', levels_ingest_max, now()) AS levels_age_s,
  dateDiff('second', max(l.ingest_ts), max(e.ts)) AS lag_s
FROM kalshi.orderbook_events e
LEFT JOIN kalshi.latest_levels l
  ON l.market_ticker = e.market_ticker
WHERE e.ts > now() - INTERVAL 2 HOUR
GROUP BY e.market_ticker
ORDER BY levels_age_s DESC
LIMIT 50;
