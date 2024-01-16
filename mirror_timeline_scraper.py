from mastodon import Mastodon
import pandas as pd
from datetime import datetime
import mysql.connector
import time
import os
import re

start_time = time.time()

print("Scraping session started at: " + str(datetime.now()))

# MySQL connection details
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
database = os.getenv("DB_DATABASE")
username = os.getenv("DB_USERNAME")
password = os.getenv("DB_PASSWORD")

# Schema when creating table afresh:
create_table_query = """
    CREATE TABLE IF NOT EXISTS mirror_timelines (
        id BIGINT UNSIGNED PRIMARY KEY,
        created_at DOUBLE(14,4),
        in_reply_to_id TINYTEXT,
        in_reply_to_account_id TINYTEXT,
        visibility TINYTEXT,
        language TINYTEXT,
        uri TINYTEXT,
        url TINYTEXT,
        replies_count BIGINT,
        reblogs_count BIGINT,
        favourites_count BIGINT,
        edited_at TINYTEXT,
        content MEDIUMTEXT,
        reblog MEDIUMTEXT,
        application TINYTEXT,
        media_type TINYTEXT,
        media_url TINYTEXT,
        account_id BIGINT,
        account_username TINYTEXT,
        account_acct TINYTEXT,
        scraped_at DOUBLE(14,4),
        instance_base_url TINYTEXT
    )
"""

import re

def preprocess_timelines(t):
  # retain_columns = ['id', 'created_at', 'in_reply_to_id', 'in_reply_to_account_id', 'visibility', 'language', 'uri', 'url', 'replies_count',
  #                     'reblogs_count', 'favourites_count', 'edited_at', 'content', 'reblog', 'application', 'media_type', 'media_url', 'account_id', 'account_username', 'account_acct', 'scraped_at', 'instance_base_url']
  retain_columns = ['id', 'created_at', 'visibility', 'language', 'uri', 'url', 'replies_count',
                      'reblogs_count', 'favourites_count', 'edited_at', 'content', 'reblog', 'application', 'media_type', 'media_url', 'account_id', 'account_username', 'account_acct', 'scraped_at', 'instance_base_url']
  tdf = pd.DataFrame(t) #convert timeline data to dataframe
  tdf['scraped_at']=round(datetime.now().timestamp())
  tdf['created_at']=tdf['created_at'].apply(lambda x: x.timestamp())
  tdf['application']=tdf['application'].apply(lambda x: str(x))
  #tdf['media_attachments']=tdf['media_attachments'].apply(lambda x: str(x))
  #tdf['media_id'] = tdf['media_attachments'].apply(lambda x: x[0]['id'] if len(x) > 0 else None)
  #tdf['media_id'] = tdf['media_attachments'].apply(lambda x: x[0]['id'] if isinstance(x, list) and len(x) > 0 else None)
  tdf['media_type'] = tdf['media_attachments'].apply(lambda x: x[0]['type'] if len(x) > 0 else None)
  tdf['media_url'] = tdf['media_attachments'].apply(lambda x: x[0]['url'] if len(x) > 0 else None)
  tdf.drop(columns='media_attachments', inplace=True)
  tdf['account_id'] = tdf['account'].apply(lambda x: x['id'])
  tdf['account_username'] = tdf['account'].apply(lambda x: x['username'])
  tdf['account_acct'] = tdf['account'].apply(lambda x: x['acct'])
  tdf['instance_base_url'] = tdf['uri'].apply(lambda x: re.sub("/users/.*", "", x))
  tdf.drop(columns='account', inplace=True)
  tdf = tdf[retain_columns]
  return tdf


#test_tdf = pd.DataFrame(timeline)
#re.sub("/users/.*", "", x)
#test = re.sub("/users/.*", "", test_tdf['uri'][1])

# head, sep, tail = test_tdf['uri'][1].partition("/users/")
# head, _, _ = test_tdf['uri'][1].partition("/users/")

dbconn = mysql.connector.connect(
    host=host,
    port=port,
    user=username,
    password=password,
    database=database
)


instances = pd.read_sql_query("SELECT instance_base_url FROM mirror_verses", dbconn)

#row = instances.iloc[0]

for index, row in instances.iterrows():
  print("Scraping timeslines from instance: " + str(row['instance_base_url']))
  # Initiate scraping session with token from random user -- probably should use my admin account when I figure out how to remove all rate limits
  user_token = pd.read_sql_query("SELECT token FROM mirror_accounts WHERE type = 'bot' AND instance_base_url = '" + str(row['instance_base_url']) + "' ORDER BY RAND() LIMIT 1", dbconn)["token"].values[0]
  mastodon = Mastodon(access_token = user_token, api_base_url = str(row['instance_base_url']))
  
  cursor = dbconn.cursor()
  query = f"SHOW TABLES LIKE 'mirror_timelines'"
  cursor.execute(query)
  table_exists = cursor.fetchone() is not None
  cursor.close()
  
  #need to distinguish between first-scrape of a given instance and initialization of the table in general
  if table_exists:
      if pd.read_sql_query('SELECT COUNT(id) FROM mirror_timelines', dbconn)['COUNT(id)'][0] > 0:
          scrape_type = 'update'
      else:
          scrape_type = 'init'
  else:
      scrape_type = 'init'
      #create table
      cursor = dbconn.cursor()
      cursor.execute(create_table_query)
      cursor.close()
  
  if scrape_type == 'update':
      since_id_from_db = pd.read_sql_query("SELECT id FROM mirror_timelines WHERE instance_base_url = '" + str(row['instance_base_url']) + "' ORDER BY id DESC;", dbconn)['id'][0:40].tolist()
      if len(since_id_from_db)>0:
        print("Updating mirror_timelines back to since_id: " + str(max(since_id_from_db)))
      else:
        print("This instance still needs an init scrape...")
        scrape_type = 'init'
  
  i = 0
  while_condition = True
  while while_condition:
  #while (i < 5):
      i += 1
      print("Batch number: " + str(i))
      if i == 1:
              t = mastodon.timeline('local', limit = 40)
      else: 
              t = mastodon.timeline('local', limit = 40, max_id = tdf.id[39])
      
      
      tdf = preprocess_timelines(t)
      print(tdf)
      
      if scrape_type == 'init':
          while_condition = (tdf.shape[0] == 40)
      if scrape_type == 'update':
          while_condition = not set(since_id_from_db).intersection(set(tdf.id.tolist()))
          if not while_condition:
              print('Update has reached last loop, because at least one of the ids of the current batch of toots is already in the database.')
              tdf = tdf[tdf['id'] > max(since_id_from_db)]
      
      
      
      if tdf.shape[0] == 0:
          print("No new statuses to scrape!  Stopping.")
          break
      
      # save to db
      cursor = dbconn.cursor()
      
      # Insert the DataFrame into the 'mirror_timelines' table
      # for _, row in tdf.iterrows():
      #     insert_query = """
      #         INSERT INTO mirror_timelines (id, created_at, in_reply_to_id, in_reply_to_account_id, visibility, language, uri, url, replies_count, reblogs_count, favourites_count, edited_at, content, reblog, application, media_type, media_url, account_id, account_username, account_acct, scraped_at, instance_base_url)
      #         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
      #     """
      #     values = tuple(row)
      #     cursor.execute(insert_query, values)
      
      
      for _, row in tdf.iterrows():
          insert_query = """
              INSERT INTO mirror_timelines (id, created_at, visibility, language, uri, url, replies_count, reblogs_count, favourites_count, edited_at, content, reblog, application, media_type, media_url, account_id, account_username, account_acct, scraped_at, instance_base_url)
              VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
          """
          values = tuple(row)
          cursor.execute(insert_query, values)
      
      # Commit the changes and close the connection
      dbconn.commit()
      cursor.close()
      #time.sleep(1)


dbconn.close()  

end_time = time.time()
end_time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end_time))
print(f"Script completed at: {end_time_str}")
runtime = end_time - start_time
print(f"Script completed in {runtime:.2f} seconds.")
