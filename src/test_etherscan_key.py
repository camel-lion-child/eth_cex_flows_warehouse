# This script is used to test whether an Etherscan API key works correctly by retrieving the ETH balance of a specific wallet address 
# using the Etherscan V2 API.

# Ce script permet de tester si une clé API Etherscan fonctionne correctement en récupérant le solde ETH d’une adresse 
# via l’API Etherscan V2.

import os
import requests
from dotenv import load_dotenv #to securely load API keys from a .env file

print(">>> Running test_etherscan_key.py (USING V2 API)")

# Load environment variables
load_dotenv()
api_key = os.getenv("ETHERSCAN_API_KEY")

print(">>> Loaded ETHERSCAN_API_KEY:", repr(api_key))

# Ensures the API key exists before continuing
# Stops execution if missing (fail fast)
if not api_key:
    print("!!! ERROR: api_key is None. Check your .env file.")
    raise SystemExit(1)

url = "https://api.etherscan.io/v2/api" #Etherscan V2 API base URL

# REST APIs with query parameters
params = {
    "chainid": 1,
    "module": "account",
    "action": "balance",
    "address": "0xde0b295669a9fd93d5f28d9ec85e40f4cb697bae",
    "apikey": api_key,
}

print(">>> Sending request to Etherscan V2...")
response = requests.get(url, params=params) # send request to Etherscan
print(">>> HTTP status:", response.status_code)
print(">>> Raw response (from V2):")
print(response.text)

