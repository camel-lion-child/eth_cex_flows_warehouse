import os
import time
import requests
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime

# ✅ V2 endpoint
ETHERSCAN_V2_BASE = "https://api.etherscan.io/v2/api"
CHAIN_ID = 1  # Ethereum mainnet

DUNE_PATH = "data/processed/dune/cex_eth_flows_daily.csv"
OUT_PATH = "data/processed/etherscan/network_sample_daily.csv"


def get_block_by_time(ts: int, api_key: str) -> int:
    """
    V2: module=block&action=getblocknobytime + chainid
    Docs: Get Block Number by Timestamp
    """
    params = {
        "chainid": CHAIN_ID,
        "module": "block",
        "action": "getblocknobytime",
        "timestamp": ts,
        "closest": "before",
        "apikey": api_key,
    }
    r = requests.get(ETHERSCAN_V2_BASE, params=params, timeout=30)
    data = r.json()
    if data.get("status") != "1":
        raise RuntimeError(f"getblocknobytime failed: {data}")
    return int(data["result"])


def get_block_detail(block_number: int, api_key: str) -> dict:
    """
    V2: module=proxy&action=eth_getBlockByNumber + chainid
    Docs: eth_getBlockByNumber
    """
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


def to_int_hex(x: str) -> int:
    return int(x, 16)


def ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


if __name__ == "__main__":
    print("🚀 Starting Etherscan V2 network sample fetch...")
    print("📌 DUNE_PATH =", os.path.abspath(DUNE_PATH))
    print("📌 OUT_PATH  =", os.path.abspath(OUT_PATH))
    print("🔗 ETHERSCAN_V2_BASE =", ETHERSCAN_V2_BASE, "| chainid =", CHAIN_ID)

    load_dotenv()
    api_key = os.getenv("ETHERSCAN_API_KEY")
    if not api_key:
        raise ValueError("Missing ETHERSCAN_API_KEY in .env")

    if not os.path.exists(DUNE_PATH):
        raise FileNotFoundError(f"Dune file not found: {DUNE_PATH}")

    dune = pd.read_csv(DUNE_PATH)
    if "day" not in dune.columns:
        raise ValueError(f"Missing 'day' column in {DUNE_PATH}. Columns: {list(dune.columns)}")

    dune["day"] = pd.to_datetime(dune["day"], errors="coerce").dt.date
    dune = dune[dune["day"].notna()].copy()

    days = sorted(dune["day"].unique())
    if not days:
        raise ValueError(f"No valid days found in {DUNE_PATH} after parsing.")

    print(f"📅 Found {len(days)} days from Dune: {days[0]} → {days[-1]}")

    rows = []
    ensure_parent_dir(OUT_PATH)

    for i, d in enumerate(days, start=1):
        ts = int(datetime(d.year, d.month, d.day, 12, 0, 0).timestamp())  # 12:00 UTC sample
        print(f"\n[{i}/{len(days)}] 📆 {d} @12:00 UTC (ts={ts})")

        try:
            block_no = get_block_by_time(ts, api_key)
            block = get_block_detail(block_no, api_key)

            gas_used = to_int_hex(block["gasUsed"])
            gas_limit = to_int_hex(block["gasLimit"])
            base_fee_wei = to_int_hex(block.get("baseFeePerGas", "0x0"))
            tx_count = len(block.get("transactions", []) or [])

            rows.append(
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

            print(f"   ✅ block={block_no} txs={tx_count} base_fee_gwei={base_fee_wei/1e9:.2f}")

        except Exception as e:
            print(f"   ❌ Failed for day {d}: {e}")

        time.sleep(0.2)

    # ✅ Fix KeyError when rows empty
    if not rows:
        raise RuntimeError(
            "No rows collected from Etherscan V2. "
            "Check API key permissions/rate limits and try again."
        )

    out_df = pd.DataFrame(rows).sort_values("day").reset_index(drop=True)
    out_df.to_csv(OUT_PATH, index=False)

    print(f"\n✅ Saved {len(out_df)} rows → {OUT_PATH}")
    print("📌 Saved file exists?", os.path.exists(OUT_PATH))

