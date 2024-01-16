
from mastodon import Mastodon
import pandas as pd
import mysql.connector
from datetime import datetime
import random
import time
import requests
import os

##MySQL connection details
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
database = os.getenv("DB_DATABASE")
username = os.getenv("DB_USERNAME")
password = os.getenv("DB_PASSWORD")

dbconn = mysql.connector.connect(
      host=host,
      port=port,
      user=username,
      password=password,
      database=database
      )

#accounts = pd.read_sql_query("SELECT * FROM mirror_accounts WHERE type = 'bot'", dbconn)
accounts = pd.read_sql_query("SELECT * FROM mirror_accounts", dbconn)

print(str(accounts.shape[0]) + " accounts to wipe!")

for account_index, account_row in accounts.iterrows():
    time.sleep(.01)
    print("Checking account: " + str(account_row["name"]) + " on instance: " + str(account_row["instance_base_url"]))
    mastodon = Mastodon(access_token = account_row["token"], api_base_url = account_row["instance_base_url"])
    these_statuses = mastodon.account_statuses(account_row["id"])
    these_statuses_ids = [item['id'] for item in these_statuses]
    print("Statuses to purge: " + str(len(these_statuses_ids)))
    if len(these_statuses_ids)>0:
      for status_id in these_statuses_ids:
        mastodon.status_delete(status_id)
        time.sleep(.01)
    else:
      print("Continuing...")
      continue

# Clear tables
# Create a cursor object
cursor = dbconn.cursor()

# List of table names to delete entries from
table_names = ["mirror_bot_posted", "mirror_timelines", "mirror_likes", "mirror_follows"]  # Add your table names here

# Loop through the table names and delete all entries from each one
for table_name in table_names:
    try:
        cursor.execute(f"DELETE FROM {table_name}")
        print(f"All entries from table {table_name} deleted successfully.")
    except mysql.connector.Error as err:
        print(f"Error deleting entries from {table_name}: {err}")

# Commit the changes
dbconn.commit()
cursor.close()

#pd.read_sql_query("SHOW TABLES;", dbconn)

dbconn.close()


