-- Compare max ingest_ts between latest_levels and latest_levels_v2 per market
SELECT
  e.market_ticker,
  max(e.ts) AS events_max,
  max(l1.ingest_ts) AS levels_v1_ingest_max,
  max(l2.ingest_ts) AS levels_v2_ingest_max,
  dateDiff('second', max(l2.ingest_ts), now()) AS levels_v2_age_s,
  dateDiff('second', max(l1.ingest_ts), max(e.ts)) AS lag_v1_s,
  dateDiff('second', max(l2.ingest_ts), max(e.ts)) AS lag_v2_s
FROM kalshi.orderbook_events e
LEFT JOIN kalshi.latest_levels l1 ON l1.market_ticker = e.market_ticker
LEFT JOIN kalshi.latest_levels_v2 l2 ON l2.market_ticker = e.market_ticker
WHERE e.ts > now() - INTERVAL 2 HOUR
GROUP BY e.market_ticker
ORDER BY levels_v2_age_s DESC
LIMIT 100;
