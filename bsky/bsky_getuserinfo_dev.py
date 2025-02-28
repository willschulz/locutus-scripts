import requests
import mysql.connector
import os

db_host = os.getenv("DB_HOST")
db_port = int(os.getenv("DB_PORT"))
db_database = os.getenv("DB_DATABASE")
db_username = os.getenv("DB_USERNAME")
db_password = os.getenv("DB_PASSWORD")

BASE_URL = "https://public.api.bsky.app/xrpc/app.bsky.actor.getProfile"

params = {
    "actor": "did:plc:jpyfeawwwwj73jypx5xv3jh7"
}

headers = {
    "Accept-Language": "en"
}

response = requests.get(BASE_URL, params=params, headers=headers)

if response.status_code == 200:
    print(response.json())  # Print the profile data
else:
    print(f"Error: {response.status_code}, {response.text}")  # Handle errors

