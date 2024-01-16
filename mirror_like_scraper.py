# status-oriented like scraper

from mastodon import Mastodon
import pandas as pd
from datetime import datetime
import mysql.connector
import time
import random
import os
import traceback

start_time = time.time()

# MySQL connection details
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
database = os.getenv("DB_DATABASE")
username = os.getenv("DB_USERNAME")
password = os.getenv("DB_PASSWORD")

# Schema when creating table afresh:
create_table_query = """
    CREATE TABLE IF NOT EXISTS mirror_likes (
        id BIGINT,
        acct TINYTEXT,
        liked_status_id BIGINT,
        scraped_at TINYTEXT,
        lid CHAR(255) PRIMARY KEY,
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

print('Started scraping likes at: ' + str(datetime.now()))

days_back = 3
time_at_start = datetime.now()

#todo: do for both instances

instances = pd.read_sql_query("SELECT instance_base_url FROM mirror_verses", dbconn)

for index, row in instances.iterrows():
  print("Scraping timeslines from instance: " + str(row['instance_base_url']))
  # Initiate scraping session with token from random user -- probably should use my admin account when I figure out how to remove all rate limits
  user_token = pd.read_sql_query("SELECT token FROM mirror_accounts WHERE type = 'bot' AND instance_base_url = '" + str(row['instance_base_url']) + "' ORDER BY RAND() LIMIT 1", dbconn)["token"].values[0]
  #user_token = pd.read_sql_query("SELECT token FROM accounts WHERE type = 'bot' ORDER BY RAND() LIMIT 1", dbconn)["token"].values[0]
  mastodon = Mastodon(access_token = user_token, api_base_url = str(row['instance_base_url']))
  
  cursor = dbconn.cursor()
  query = f"SHOW TABLES LIKE 'mirror_likes'"
  cursor.execute(query)
  table_exists = cursor.fetchone() is not None
  cursor.close()
  
  if table_exists:
      if pd.read_sql_query('SELECT COUNT(id) FROM mirror_likes', dbconn)['COUNT(id)'][0] > 0:
          scrape_type = 'update'
      else:
          scrape_type = 'init'
  else:
      scrape_type = 'init'
      cursor = dbconn.cursor()
      cursor.execute(create_table_query)
      cursor.close()
  
  ##### come back to this XxXXX change this code to get IDs of all statuses already known to have been liked by user in past X days -- maybe should put it inside the user-level loop
  # if scrape_type == 'update':
  #     con = sqlite3.connect(like_database_path)
  #     already_scraped_favorite_ids = pd.read_sql_query('SELECT id FROM mirror_likes ORDER BY id DESC', con)['id'][0:40].tolist()
  #     con.close()
  #     print("Updating mirror_likes back to since_id: " + max(since_id_from_db))
  #####
  
  # get ids to check
  now_timestamp=datetime.now().timestamp()
  ids_to_check = pd.read_sql_query("SELECT id FROM mirror_timelines WHERE instance_base_url = '" + str(row['instance_base_url']) + "' AND created_at > " + str(now_timestamp-60*60*24*days_back), dbconn).id.to_list()
  ids_to_check
  
  statuses_remaining = ids_to_check
  while len(statuses_remaining) > 0:
      random_index = random.randint(0,len(statuses_remaining)-1)
      this_status_id = statuses_remaining[random_index]
      print('BEGIN scraping likes of status ' + str(this_status_id))
      try:
        f = mastodon.status_favourited_by(this_status_id)
      except:
        print("Couldn't scrape likes for this status.  Maybe deleted.")
        statuses_remaining = statuses_remaining[:random_index] + statuses_remaining[random_index + 1:]
        continue
      fdf = pd.DataFrame(f)#.applymap(str)
      print(str(fdf.shape[0]) + ' likes scraped...')
      if fdf.shape[0]>0:
              fdf=fdf[['id', 'acct']] #what is 'id' if not the id of the liked status? drop if not useful
              fdf['liked_status_id']=this_status_id
              fdf['scraped_at']=datetime.now().timestamp()
              if table_exists:
                  print('Like table exists, checking whether any previously scraped likers for this tweet...')
                  #look up any previous likes of this tweet in the database:
                  previously_scraped_likers = pd.read_sql_query('SELECT acct FROM mirror_likes WHERE liked_status_id = ' + str(this_status_id), dbconn)
                  print(str(previously_scraped_likers.shape[0]) + ' previously scraped likers exist for this tweet.')
                  if previously_scraped_likers.shape[0]>0:
                    print('Removing duplicate likers from fdf_new...')
                    fdf_new = fdf[~fdf['acct'].isin(previously_scraped_likers.acct)]
                  else:
                    fdf_new = fdf
              else:
                fdf_new = fdf
              print(str(fdf_new.shape[0]) + ' new likes scraped...')
              if fdf_new.shape[0]>0:
                fdf_new['scraped_at'] = now_timestamp #does this break when there is more than one row of the df?
                fdf_new['lid'] = fdf_new['id'].astype(str) + '_' + fdf_new['liked_status_id'].astype(str)
                fdf_new['instance_base_url'] = str(row['instance_base_url'])
                print('Saving to database...')
                cursor = dbconn.cursor()
                for _, row in fdf_new.iterrows():
                  insert_query = """
                      INSERT INTO mirror_likes (id, acct, liked_status_id, scraped_at, lid, instance_base_url)
                      VALUES (%s, %s, %s, %s, %s, %s)
                  """
                  values = tuple(row)
                  cursor.execute(insert_query, values)
                # Commit the changes and close the connection
                dbconn.commit()
                cursor.close()
                table_exists = True
              else:
                print('Moving on since no new likes scraped...')
      time.sleep(.1)
      statuses_remaining = statuses_remaining[:random_index] + statuses_remaining[random_index + 1:] #maybe make conditional on detecting some likes (and add a time constraint on overall loop)
      print(str(len(statuses_remaining)) + " statuses remaining")
    
print("Done scraping favorites for this scraping session!")
print('Finished scraping likes at' + str(datetime.now()))

dbconn.close()

end_time = time.time()
end_time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end_time))
print(f"Script completed at: {end_time_str}")
runtime = end_time - start_time
print(f"Script completed in {runtime:.2f} seconds.")
