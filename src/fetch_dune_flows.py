"""
Execute a Dune query to retrieve CEX ETH flow data and store raw results.
Normalize the output into a structured dataset with daily net flows.
Save results as a CSV for downstream analysis.

Exécute une requête Dune pour récupérer les flux ETH des CEX et stocker les données brutes.
Normalise les résultats en un dataset structuré avec des flux nets journaliers.
Sauvegarde les données dans un CSV pour analyse.
"""

import os
import time
import requests
import pandas as pd
from dotenv import load_dotenv

QUERY_ID = 5971722  #Dune query ID used to fetch CEX ETH flow data
BASE_URL = "https://api.dune.com/api/v1"

RAW_JSON_PATH = "data/raw/dune/cex_eth_flows_raw.json"
CSV_OUT_PATH = "data/processed/dune/cex_eth_flows_daily.csv"

#create output folders if they don't exist yet
os.makedirs("data/raw/dune", exist_ok=True)
os.makedirs("data/processed/dune", exist_ok=True)


def get_api_key():
    load_dotenv() #load environment variables from .env file
    api_key = os.getenv("DUNE_API_KEY")
    if not api_key:
        raise ValueError("DUNE_API_KEY not found in .env")
    return api_key #stop execution if API key is missing


def run_query(api_key: str) -> str:
    url = f"{BASE_URL}/query/{QUERY_ID}/execute" #build endpoint to execute Dune query
    headers = {"x-dune-api-key": api_key}
    resp = requests.post(url, headers=headers)
    data = resp.json()
    execution_id = data.get("execution_id")
    if not execution_id: #raise error if the qyery didn't start correctly
        raise RuntimeError(f"Failed to start query: {data}")
    return execution_id


def wait_for_results(api_key: str, execution_id: str, poll_interval: float = 3.0):
    url = f"{BASE_URL}/execution/{execution_id}/status" #build endpoint to check exectution status
    headers = {"x-dune-api-key": api_key}

    print(f" Waiting for Dune query {execution_id} to finish...")

    #poll Dune repeatedly until query completes or fails
    while True: 
        resp = requests.get(url, headers=headers)
        data = resp.json()
        state = data.get("state") or data.get("execution_state") #Dune may return state under different field names depending on API version

        print(f" state = {state}")

        if state in ("QUERY_STATE_COMPLETED", "COMPLETED"):
            break
        if state in ("QUERY_STATE_FAILED", "FAILED"):
            raise RuntimeError(f"Dune query failed: {data}")

        time.sleep(poll_interval)


def fetch_results(api_key: str, execution_id: str) -> dict: #build endpoint to retrieve final query results
    url = f"{BASE_URL}/execution/{execution_id}/results"
    headers = {"x-dune-api-key": api_key} #authentication header
    resp = requests.get(url, headers=headers)
    data = resp.json()
    return data


def normalize_results_to_df(results: dict) -> pd.DataFrame:
    #validate that the reponse contains tabular rows
    if "result" not in results or "rows" not in results["result"]:
        raise ValueError(f"Unexpected results format: {results}")

    rows = results["result"]["rows"] #extract row level data from Dune response
    df = pd.DataFrame(rows)


    for col in ["day", "date", "block_date"]:
        if col in df.columns:
            df["day"] = pd.to_datetime(df[col]).dt.date
            break

    #detect inflow and outflow columns automatically
    inflow_cols = [c for c in df.columns if "inflow" in c]
    outflow_cols = [c for c in df.columns if "outflow" in c]

    #compute daily net ETH flow if both inflow and outflow exist
    if inflow_cols and outflow_cols:
        inflow_col = inflow_cols[0]
        outflow_col = outflow_cols[0]
        df["netflow_eth"] = df[inflow_col].astype(float) - df[outflow_col].astype(float)

    return df


if __name__ == "__main__":
    api_key = get_api_key()

    print("Running Dune query...")
    execution_id = run_query(api_key) #launch query execution on Dune
    print(f"   → execution_id = {execution_id}")

    wait_for_results(api_key, execution_id) #wait until Dune finishes processing query

    print("Fetching results...")
    results = fetch_results(api_key, execution_id) #download final query output

    #save raw API response for traceability / debugging
    with open(RAW_JSON_PATH, "w") as f:
        f.write(str(results))

    print("Normalizing to DataFrame...")
    df = normalize_results_to_df(results) #convert raw Dune result into clean analysis ready table

    df.to_csv(CSV_OUT_PATH, index=False) #save processed dataset as csv
    print(f"DONE → {CSV_OUT_PATH}")
    print(">>> Running fetch_dune_flows.py (USING V2 API)") 
