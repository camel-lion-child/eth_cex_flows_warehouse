

# ETH CEX Flows Warehouse (Dune + Binance + Etherscan)

ETH CEX Flows Warehouse is a lightweight analytics engineering project designed to study Ethereum liquidity dynamics across centralized exchanges.

The project focuses on understanding how liquidity positioning and network conditions evolve relative to market price, using a clean and reproducible data workflow. It builds a DuckDB warehouse combining:
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
