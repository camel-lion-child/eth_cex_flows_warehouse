# ETH CEX Flows Analytics Warehouse

(Dune • Binance • Etherscan)

# Overview

This project builds a lightweight analytics warehouse to study Ethereum liquidity flows across centralized exchanges (CEX) and their relationship with price action and network conditions.

The goal is not trading signals, but structuring raw on-chain & market data into a clean, reproducible analytical layer suitable for exploratory analysis and future modeling.

---

##Tech Stack

- Python — data ingestion, processing, orchestration.

- DuckDB — analytical warehouse (OLAP, columnar).

- Dune Analytics — on-chain aggregated CEX flow data.

- Binance API — market price & returns.

- Etherscan API — Ethereum network activity metrics.

- Pandas / NumPy — data transformation & aggregation.

- Jupyter Notebook — exploratory data analysis (EDA).

---

##Data Sources

- Dune — aggregated ETH CEX flows (inflow, outflow, netflow).

- Binance — ETH daily price & returns.

- Etherscan (sampled) — network activity metrics (base fee, tx count, gas usage ratio).

---

##Architecture

- Raw data ingestion via Python scripts.

- Daily-grain processed tables.

- Centralized DuckDB warehouse for analytics.

- Exploration via Jupyter Notebook (EDA).

Raw APIs / Dune
      ↓
Daily processed tables
      ↓
DuckDB analytics warehouse
      ↓
EDA & liquidity analysis

---

##Pipeline

- Fetch raw data from Dune, Binance, Etherscan.

- Normalize & aggregate at daily level.

- Build DuckDB warehouse (warehouse/eth_cex.duckdb).

- Run exploratory liquidity analysis.

---

##Key Analyses

- ETH inflow / outflow dynamics across CEXs.

- Netflow behavior around price moves.

- Relationship between network congestion & exchange flows.

- Liquidity positioning vs market regimes.

---

##Why This Project

This project reflects a Data Engineering → Analytics Engineering mindset applied to DeFi / crypto markets:

- Focus on data modeling, not dashboards only.

- Reproducible pipelines.

- Analytics-ready warehouse design.

- Extensible toward risk analysis or modeling.

---

##Future Extensions

- Incremental ingestion.
  
- Partitioned Parquet storage.
  
- On-chain risk & liquidity stress analysis.

---
