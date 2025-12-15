import os
import duckdb

os.makedirs("warehouse", exist_ok=True)
DB_PATH = "warehouse/eth_cex.duckdb"
con = duckdb.connect(DB_PATH)

con.execute("""
CREATE OR REPLACE TABLE fact_cex_eth_flows AS
SELECT
    CAST(day AS DATE) AS day,
    eth_inflow,
    eth_outflow,
    netflow_eth
FROM read_csv_auto('data/processed/dune/cex_eth_flows_daily.csv');
""")

con.execute("""
CREATE OR REPLACE TABLE fact_eth_price AS
SELECT
    CAST(day AS DATE) AS day,
    price_usd,
    daily_return,
    rolling_vol_7d
FROM read_csv_auto('data/processed/binance/eth_price_daily.csv');
""")

con.execute("""
CREATE OR REPLACE TABLE fact_eth_network_sample AS
SELECT
    CAST(day AS DATE) AS day,
    sample_block_number,
    block_tx_count,
    block_gas_used,
    block_gas_limit,
    block_gas_used_ratio,
    block_base_fee_gwei
FROM read_csv_auto('data/processed/etherscan/network_sample_daily.csv');
""")

con.execute("""
CREATE OR REPLACE VIEW v_cex_eth_macro_with_network AS
SELECT
    f.day,
    f.eth_inflow,
    f.eth_outflow,
    f.netflow_eth,
    n.sample_block_number,
    n.block_tx_count,
    n.block_gas_used_ratio,
    n.block_base_fee_gwei,
    p.price_usd,
    p.daily_return,
    p.rolling_vol_7d
FROM fact_cex_eth_flows f
LEFT JOIN fact_eth_network_sample n
    ON f.day = n.day
LEFT JOIN fact_eth_price p
    ON f.day = p.day;
""")

print("DuckDB warehouse built at:", DB_PATH)
print(con.execute("SELECT COUNT(*) AS n FROM v_cex_eth_macro_with_network").df())

con.close()

