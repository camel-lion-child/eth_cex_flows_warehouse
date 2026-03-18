"""
Fetch daily Ethereum network metrics from Etherscan and enrich Dune data.
Samples one block per day (~12:00 UTC) and extracts key indicators (tx, gas, base fee).
Outputs a structured CSV for analysis.

Récupère des métriques réseau Ethereum via Etherscan et enrichit les données Dune.
Échantillonne un bloc par jour (~12h UTC) et extrait des indicateurs clés (tx, gas, base fee).
Génère un CSV structuré pour l’analyse.
"""

import os
import time
import requests
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime


ETHERSCAN_V2_BASE = "https://api.etherscan.io/v2/api"
CHAIN_ID = 1  # Ethereum mainnet chain ID

DUNE_PATH = "data/processed/dune/cex_eth_flows_daily.csv"
OUT_PATH = "data/processed/etherscan/network_sample_daily.csv"


def get_block_by_time(ts: int, api_key: str) -> int: # retrieve the Ethereum block number closest to a given Unix timestamp
    # mapping a daily timestamp to a real block number on Ethereum
    params = {
        "chainid": CHAIN_ID,
        "module": "block",
        "action": "getblocknobytime",
        "timestamp": ts, #Unix timestamp
        "closest": "before",
        "apikey": api_key,
    }
    r = requests.get(ETHERSCAN_V2_BASE, params=params, timeout=30)
    data = r.json()
    if data.get("status") != "1": # stop execution if the API response in invalid
        raise RuntimeError(f"getblocknobytime failed: {data}")
    return int(data["result"])


def get_block_detail(block_number: int, api_key: str) -> dict: # retrieve detailed information for a given Ethereum block
    #fetching gas used, gas limit, base fee and transactions.
   
    params = {
        "chainid": CHAIN_ID,
        "module": "proxy",
        "action": "eth_getBlockByNumber",
        "tag": hex(block_number),
        "boolean": "true",
        "apikey": api_key,
    }
    r = requests.get(ETHERSCAN_V2_BASE, params=params, timeout=30)
    data = r.json()
    if "error" in data:
        raise RuntimeError(f"eth_getBlockByNumber error: {data}")
    result = data.get("result")
    if result is None:
        raise RuntimeError(f"eth_getBlockByNumber no result: {data}")
    return result


def to_int_hex(x: str) -> int: #convert parent hexadecimal string returned by Ethereum API into an integer
    return int(x, 16)


def ensure_parent_dir(path: str) -> None: #create parent directory if it doesn't exist
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


if __name__ == "__main__":
    print("Starting Etherscan V2 network sample fetch...")
    print("DUNE_PATH =", os.path.abspath(DUNE_PATH))
    print("OUT_PATH  =", os.path.abspath(OUT_PATH))
    print("ETHERSCAN_V2_BASE =", ETHERSCAN_V2_BASE, "| chainid =", CHAIN_ID)

    load_dotenv() #environment varibales
    api_key = os.getenv("ETHERSCAN_API_KEY")
    if not api_key: #fail fast if the API key is missing
        raise ValueError("Missing ETHERSCAN_API_KEY in .env")

    if not os.path.exists(DUNE_PATH): #make sure Dune input file exists before processing
        raise FileNotFoundError(f"Dune file not found: {DUNE_PATH}")

    dune = pd.read_csv(DUNE_PATH)
    if "day" not in dune.columns:
        raise ValueError(f"Missing 'day' column in {DUNE_PATH}. Columns: {list(dune.columns)}")

    dune["day"] = pd.to_datetime(dune["day"], errors="coerce").dt.date
    dune = dune[dune["day"].notna()].copy()

    days = sorted(dune["day"].unique()) #extract unique sorted dates from Dune dataset
    if not days:
        raise ValueError(f"No valid days found in {DUNE_PATH} after parsing.")

    print(f"Found {len(days)} days from Dune: {days[0]} → {days[-1]}")

    rows = []
    ensure_parent_dir(OUT_PATH) #ensure the output folder exists before saving results

    for i, d in enumerate(days, start=1):
        ts = int(datetime(d.year, d.month, d.day, 12, 0, 0).timestamp()) # using 12:00 utc as a daily reference point to sample one representative block
        print(f"\n[{i}/{len(days)}] {d} @12:00 UTC (ts={ts})")

        try:
            block_no = get_block_by_time(ts, api_key) #find block number closest to daily timestamp
            block = get_block_detail(block_no, api_key) #retrieve block level data for that block

            gas_used = to_int_hex(block["gasUsed"]) #convert hexadecimal blockchain fields into numeric values
            gas_limit = to_int_hex(block["gasLimit"])
            base_fee_wei = to_int_hex(block.get("baseFeePerGas", "0x0"))
            tx_count = len(block.get("transactions", []) or [])

            rows.append(   #store 1 daily observation of network activity
                {
                    "day": d,
                    "sample_block_number": block_no,
                    "block_tx_count": tx_count,
                    "block_gas_used": gas_used,
                    "block_gas_limit": gas_limit,
                    "block_gas_used_ratio": (gas_used / gas_limit) if gas_limit else 0.0,
                    "block_base_fee_gwei": base_fee_wei / 1e9,
                }
            )

            print(f"   block={block_no} txs={tx_count} base_fee_gwei={base_fee_wei/1e9:.2f}")

        except Exception as e: #continue the loop even if 1 day failed
            print(f"   Failed for day {d}: {e}")

        time.sleep(0.2) #small pause to reduce the risk of hitting API rate limits


    if not rows:
        raise RuntimeError(
            "No rows collected from Etherscan V2. "
            "Check API key permissions/rate limits and try again."
        ) #stop if no data could be collected

    out_df = pd.DataFrame(rows).sort_values("day").reset_index(drop=True) #final reframe, sort by day
    out_df.to_csv(OUT_PATH, index=False)

    print(f"\nSaved {len(out_df)} rows → {OUT_PATH}")
    print("Saved file exists?", os.path.exists(OUT_PATH))

