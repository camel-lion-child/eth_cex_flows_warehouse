-- This query identifies known CEX addresses, computes daily ETH inflows and outflows from Ethereum trace data, 
-- and combines them into a single daily time series

-- Cette requête identifie les adresses connues de CEX, calcule les flux quotidiens entrants et sortants d’ETH à partir des traces Ethereum, 
-- puis les regroupe dans une seule série temporelle journalière.


-- indentify all known CEX wallet address
WITH cex AS (
  SELECT DISTINCT
    LOWER(TRY_CAST(address AS VARCHAR)) AS addr --normalize addresses to lowercase to ensure consistent matching
  FROM labels.addresses
  WHERE REGEXP_LIKE(
    LOWER(name),
    '(binance|kraken|coinbase|okx|kucoin|bybit|gate|huobi|htx|bitfinex|bitstamp|gemini|mexc)'
  )
),

-- compute ETH outflows from CEXs to non-CEX address
outflow AS (
  SELECT
    date_trunc('day', t.block_time) AS day,
    SUM(TRY_CAST(t.value AS DOUBLE) / 1e18) AS eth_outflow --sum ETH value (convert from wei to ETH by dividing by 1e18)
  FROM ethereum.traces AS t
  WHERE
    LOWER(TRY_CAST(t."from" AS VARCHAR)) IN (SELECT addr FROM cex) --transactions sent from CEX addresses
    AND LOWER(TRY_CAST(t."to" AS VARCHAR)) NOT IN (SELECT addr FROM cex) --exclude transfers between CEXs & keep only external outflows
    AND t.success = TRUE -- only sucessful transactions
    AND t.block_time >= CURRENT_TIMESTAMP - INTERVAL '30' DAY
  GROUP BY 1
),

-- compute ETH inflows to CEX from non-CEX addresses
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

-- combine inflows & outflows into a single daily dataset
SELECT
  COALESCE(o.day, i.day) AS day, -- merge date from both tables in case one side is missing
  COALESCE(eth_outflow, 0) AS eth_outflow, --replace NULL with 0 for missing values
  COALESCE(eth_inflow, 0) AS eth_inflow
FROM outflow o
FULL OUTER JOIN inflow i
  ON o.day = i.day
ORDER BY day ASC;

-- This query calculates the daily inflow and outflow of ETH to and from centralized exchanges (CEXs) over the last 30 days.
-- It identifies CEX addresses from the labels.addresses table, then aggregates the inflows and outflows from the ethereum.traces table.
-- The results are presented in a single table with columns for the day, ETH outflow, and ETH inflow.
-- The query uses a full outer join to ensure that all days are represented, even if there are no transactions on a given day.  
