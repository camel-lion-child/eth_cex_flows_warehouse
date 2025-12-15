import os
import requests
from dotenv import load_dotenv

print(">>> Running test_etherscan_key.py (USING V2 API)")

load_dotenv()
api_key = os.getenv("ETHERSCAN_API_KEY")

print(">>> Loaded ETHERSCAN_API_KEY:", repr(api_key))

if not api_key:
    print("!!! ERROR: api_key is None. Check your .env file.")
    raise SystemExit(1)

url = "https://api.etherscan.io/v2/api"

params = {
    "chainid": 1,
    "module": "account",
    "action": "balance",
    "address": "0xde0b295669a9fd93d5f28d9ec85e40f4cb697bae",
    "apikey": api_key,
}

print(">>> Sending request to Etherscan V2...")
response = requests.get(url, params=params)
print(">>> HTTP status:", response.status_code)
print(">>> Raw response (from V2):")
print(response.text)

