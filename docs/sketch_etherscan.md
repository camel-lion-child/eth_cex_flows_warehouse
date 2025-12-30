# Sketch: Etherscan Network Enrichment (API V2)

## Purpose

This project builds a small, production-flavored analytics warehouse by combining:

Dune (macro CEX ETH flows): eth_inflow, eth_outflow, netflow_eth

Binance (market context): price_usd, daily_return, rolling_vol_7d

Etherscan (on-chain network context): block-level network metrics sampled daily

The goal is not to replicate full exchange wallet labeling (Nansen/CryptoQuant style), but to demonstrate data engineering fundamentals: multi-source ingestion, normalization, modeling, warehousing, and analysis-ready views.

---

## What I Extract from Etherscan

I collect daily network metrics by sampling one representative Ethereum block per day.

- Sampling Strategy

- For each day present in Dune data, I sample the block closest before 12:00 UTC

- This yields a stable daily snapshot aligned with the Dune calendar

- Etherscan API (V2)

- Base: https://api.etherscan.io/v2/api

- chainid=1 (Ethereum mainnet)

- Endpoints used:

module=block&action=getblocknobytime
→ timestamp → block number

module=proxy&action=eth_getBlockByNumber
→ block number → block header + tx list

---

## Output Dataset

Generated file:

data/processed/etherscan/network_sample_daily.csv

Schema:
| column                 | type   | meaning                                   |
| ---------------------- | ------ | ----------------------------------------- |
| `day`                  | DATE   | daily key aligned with Dune               |
| `sample_block_number`  | INT    | representative block at ~12:00 UTC        |
| `block_tx_count`       | INT    | number of transactions (activity proxy)   |
| `block_gas_used`       | BIGINT | total gas used (load proxy)               |
| `block_gas_limit`      | BIGINT | total gas capacity                        |
| `block_gas_used_ratio` | DOUBLE | `gas_used / gas_limit` (congestion proxy) |
| `block_base_fee_gwei`  | DOUBLE | EIP-1559 base fee (fee pressure proxy)    |

---

## How It Fits in the Warehouse

Fact tables (DuckDB)

- fact_cex_eth_flows (Dune)

- fact_eth_price (Binance)

- fact_eth_network_sample (Etherscan)

---

## Analysis View

By joining all three sources at the daily level:

- macro flows (Dune)

- market context (Binance)

- network conditions (Etherscan)

We can enable analysis questions such as:

- Do strong net outflows coincide with higher base fees or congestion?

- Is netflow more correlated with on-chain activity (tx_count) or market returns?
