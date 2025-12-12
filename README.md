

# ETH CEX Flows Warehouse (Dune + Binance + Etherscan)

A small analytics engineering project that builds a DuckDB warehouse combining:
- **Dune**: market-wide ETH CEX flows (inflow/outflow/netflow)
- **Binance**: ETH daily price and returns
- **Etherscan (network sample)**: Ethereum network activity metrics (base fee, tx count, gas used ratio)

## Pipeline
1. Fetch raw data (Dune/Binance/Etherscan)
2. Process into daily tables (`data/processed/...`)
3. Build DuckDB warehouse (`warehouse/eth_cex.duckdb`)
4. EDA in `notebooks/EDA.ipynb`

## How to run
```bash
python3 src/build_warehouse.py
python3 src/explore_liquidity.py
