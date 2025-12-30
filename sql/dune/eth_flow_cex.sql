WITH cex AS (
  SELECT DISTINCT
    LOWER(TRY_CAST(address AS VARCHAR)) AS addr
  FROM labels.addresses
  WHERE REGEXP_LIKE(
    LOWER(name),
    '(binance|kraken|coinbase|okx|kucoin|bybit|gate|huobi|htx|bitfinex|bitstamp|gemini|mexc)'
  )
),

outflow AS (
  SELECT
    date_trunc('day', t.block_time) AS day,
    SUM(TRY_CAST(t.value AS DOUBLE) / 1e18) AS eth_outflow
  FROM ethereum.traces AS t
  WHERE
    LOWER(TRY_CAST(t."from" AS VARCHAR)) IN (SELECT addr FROM cex)
    AND LOWER(TRY_CAST(t."to" AS VARCHAR)) NOT IN (SELECT addr FROM cex)
    AND t.success = TRUE
    AND t.block_time >= CURRENT_TIMESTAMP - INTERVAL '30' DAY
  GROUP BY 1
),

inflow AS (
  SELECT
    date_trunc('day', t.block_time) AS day,
    SUM(TRY_CAST(t.value AS DOUBLE) / 1e18) AS eth_inflow
  FROM ethereum.traces AS t
  WHERE
    LOWER(TRY_CAST(t."to" AS VARCHAR)) IN (SELECT addr FROM cex)
    AND LOWER(TRY_CAST(t."from" AS VARCHAR)) NOT IN (SELECT addr FROM cex)
    AND t.success = TRUE
    AND t.block_time >= CURRENT_TIMESTAMP - INTERVAL '30' DAY
  GROUP BY 1
)

SELECT
  COALESCE(o.day, i.day) AS day,
  COALESCE(eth_outflow, 0) AS eth_outflow,
  COALESCE(eth_inflow, 0) AS eth_inflow
FROM outflow o
FULL OUTER JOIN inflow i
  ON o.day = i.day
ORDER BY day ASC;

-- This query calculates the daily inflow and outflow of ETH to and from centralized exchanges (CEXs) over the last 30 days.
-- It identifies CEX addresses from the labels.addresses table, then aggregates the inflows and outflows from the ethereum.traces table.
-- The results are presented in a single table with columns for the day, ETH outflow, and ETH inflow.
-- The query uses a full outer join to ensure that all days are represented, even if there are no transactions on a given day.  
