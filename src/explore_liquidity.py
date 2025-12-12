import os
import duckdb
import pandas as pd

DB_PATH = "warehouse/eth_cex.duckdb"
OUT_PATH = "data/analysis/cex_eth_liquidity_full.csv"


def load_view(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    df = con.execute("""
        SELECT *
        FROM v_cex_eth_macro_with_network
        ORDER BY day
    """).df()

    if df.empty:
        raise RuntimeError("View v_cex_eth_macro_with_network returned 0 rows.")

    # Ensure proper types
    df["day"] = pd.to_datetime(df["day"])
    for col in [
        "eth_inflow", "eth_outflow", "netflow_eth",
        "block_tx_count", "block_gas_used_ratio", "block_base_fee_gwei",
        "price_usd", "daily_return", "rolling_vol_7d"
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def print_healthcheck(df: pd.DataFrame) -> None:
    print("\n🧪 Health check:")
    print("Rows:", len(df))
    print("Date range:", df["day"].min().date(), "→", df["day"].max().date())

    cols_to_check = [
        "eth_inflow", "eth_outflow", "netflow_eth",
        "price_usd", "daily_return",
        "block_tx_count", "block_base_fee_gwei", "block_gas_used_ratio"
    ]
    present = [c for c in cols_to_check if c in df.columns]
    print("\nNon-null counts:")
    print(df[present].notna().sum())


def add_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # Next-day return
    df["next_day_return"] = df["daily_return"].shift(-1)

    # Whale-ish days proxy (network fee spikes) - optional simple label
    # You can remove this if you want.
    df["fee_spike"] = df["block_base_fee_gwei"] > df["block_base_fee_gwei"].rolling(7, min_periods=3).mean() * 1.25

    return df


def corr(a: pd.Series, b: pd.Series) -> float:
    x = pd.to_numeric(a, errors="coerce")
    y = pd.to_numeric(b, errors="coerce")
    return x.corr(y)


def print_top_days(df: pd.DataFrame, n: int = 15) -> None:
    show_cols = [
        "day", "netflow_eth", "eth_inflow", "eth_outflow",
        "price_usd", "daily_return",
        "block_base_fee_gwei", "block_tx_count", "block_gas_used_ratio"
    ]
    show_cols = [c for c in show_cols if c in df.columns]

    outflow = df.sort_values("netflow_eth", ascending=True).head(n)[show_cols]
    inflow = df.sort_values("netflow_eth", ascending=False).head(n)[show_cols]

    print(f"\n🔥 Top {n} days net OUTFLOW (most negative netflow_eth):")
    print(outflow.to_string(index=False))

    print(f"\n💧 Top {n} days net INFLOW (most positive netflow_eth):")
    print(inflow.to_string(index=False))


def print_correlations(df: pd.DataFrame) -> None:
    print("\n📊 Correlations:")
    print("Corr(netflow_eth, daily_return same day) =", round(corr(df["netflow_eth"], df["daily_return"]), 4))
    if "next_day_return" in df.columns:
        print("Corr(netflow_eth, next_day_return)      =", round(corr(df["netflow_eth"], df["next_day_return"]), 4))

    if "block_base_fee_gwei" in df.columns:
        print("Corr(netflow_eth, block_base_fee_gwei)  =", round(corr(df["netflow_eth"], df["block_base_fee_gwei"]), 4))
    if "block_tx_count" in df.columns:
        print("Corr(netflow_eth, block_tx_count)       =", round(corr(df["netflow_eth"], df["block_tx_count"]), 4))
    if "block_gas_used_ratio" in df.columns:
        print("Corr(netflow_eth, block_gas_used_ratio) =", round(corr(df["netflow_eth"], df["block_gas_used_ratio"]), 4))


def save_full(df: pd.DataFrame) -> None:
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    df_out = df.copy()
    df_out["day"] = df_out["day"].dt.date  # nicer for CSV
    df_out.to_csv(OUT_PATH, index=False)
    print(f"\n✅ Saved full joined series to {OUT_PATH}")


if __name__ == "__main__":
    print(">>> Running explore_liquidity.py (macro + price + etherscan network)")

    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"DuckDB not found: {DB_PATH}. Run build_warehouse.py first.")

    con = duckdb.connect(DB_PATH)

    df = load_view(con)
    con.close()

    print_healthcheck(df)

    df = add_features(df)

    save_full(df)

    print_top_days(df, n=15)

    print_correlations(df)

    # Optional: quick fee spike overview
    if "fee_spike" in df.columns:
        spike_days = df[df["fee_spike"] == True][["day", "block_base_fee_gwei", "netflow_eth", "daily_return"]].copy()
        if not spike_days.empty:
            print("\n⚡ Fee spike days (base fee > 1.25x 7d mean):")
            spike_days["day"] = spike_days["day"].dt.date
            print(spike_days.to_string(index=False))
