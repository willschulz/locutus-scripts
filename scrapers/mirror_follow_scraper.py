# follows scraper

from mastodon import Mastodon
import pandas as pd
from datetime import datetime
import mysql.connector
import time
import random
import os

start_time = time.time()

# MySQL connection details
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
database = os.getenv("DB_DATABASE")
username = os.getenv("DB_USERNAME")
password = os.getenv("DB_PASSWORD")

# Schema when creating table afresh:
create_table_query = """
    CREATE TABLE IF NOT EXISTS mirror_follows (
        id TINYTEXT,
        follower TINYTEXT,
        scraped_at TINYTEXT,
        follow_scrape_id CHAR(255) PRIMARY KEY,
        instance_base_url TINYTEXT
    )
"""

dbconn = mysql.connector.connect(
    host=host,
    port=port,
    user=username,
    password=password,
    database=database
)

print('Started scraping follows at' + str(datetime.now()))

instances = pd.read_sql_query("SELECT instance_base_url FROM mirror_verses", dbconn)

for instance_index, instance_row in instances.iterrows():
  print("Scraping follows from instance: " + str(instance_row['instance_base_url']))
  # Initiate scraping session with token from random user -- probably should use my admin account when I figure out how to remove all rate limits
  user_token = pd.read_sql_query("SELECT token FROM mirror_accounts WHERE type = 'bot' AND instance_base_url = '" + str(instance_row['instance_base_url']) + "' ORDER BY RAND() LIMIT 1", dbconn)["token"].values[0]
  mastodon = Mastodon(access_token = user_token, api_base_url = str(instance_row['instance_base_url']))

  cursor = dbconn.cursor()
  query = f"SHOW TABLES LIKE 'mirror_follows'"
  cursor.execute(query)
  table_exists = cursor.fetchone() is not None
  cursor.close()
  
  if table_exists:
      if pd.read_sql_query('SELECT COUNT(id) FROM mirror_follows', dbconn)['COUNT(id)'][0] > 0:
          scrape_type = 'update'
      else:
          scrape_type = 'init'
  else:
      scrape_type = 'init'
      cursor = dbconn.cursor()
      cursor.execute(create_table_query)
      cursor.close()
  
  # get user ids to check
  ids_to_check = pd.read_sql_query("SELECT id FROM mirror_accounts WHERE instance_base_url = '" + str(instance_row['instance_base_url']) + "';", dbconn).id.to_list()
  
  accounts_remaining = ids_to_check
  while len(accounts_remaining) > 0:
      random_index = random.randint(0,len(accounts_remaining)-1)
      this_user_id = accounts_remaining[random_index]
      print('BEGIN scraping followers of user ' + str(this_user_id))
      followers = mastodon.account_followers(this_user_id)
      if hasattr(followers, "_pagination_next"):
        followers = mastodon.fetch_remaining(followers)
      followers = [inner_list['id'] for inner_list in followers]
      #len(followers)
      
      #followers #remove -99?
      
      follow_df = pd.DataFrame({'id': this_user_id, 'follower': followers, 'scraped_at': datetime.now().timestamp()})
      follow_df['follow_scrape_id'] = follow_df['id'].astype(str) + '_' + follow_df['follower'].astype(str) + '_' + follow_df['scraped_at'].astype(str)
      
      follow_df['instance_base_url'] = str(instance_row['instance_base_url'])
      
      #follow_df
      if follow_df.shape[0]>0:
        print('Saving to database...')
        cursor = dbconn.cursor()
        for _, row in follow_df.iterrows():
          insert_query = """
              INSERT INTO mirror_follows (id, follower, scraped_at, follow_scrape_id, instance_base_url)
              VALUES (%s, %s, %s, %s, %s)
          """
          values = tuple(row)
          cursor.execute(insert_query, values)
        dbconn.commit()
        cursor.close()
      time.sleep(.1)
      accounts_remaining = accounts_remaining[:random_index] + accounts_remaining[random_index + 1:] #maybe add a time constraint on overall loop
      print(str(len(accounts_remaining)) + " accounts remaining")
    
print("Done scraping follows for this scraping session!")
print('Finished scraping follows at' + str(datetime.now()))

dbconn.close()

end_time = time.time()
end_time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end_time))
print(f"Script completed at: {end_time_str}")
runtime = end_time - start_time
print(f"Script completed in {runtime:.2f} seconds.")
