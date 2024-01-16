# ~/miniconda3/envs/locutus/bin/python ~/scripts/engagement_bots.py

minutes = 59 #make 59 in actual implementation

interval_lower = 30
interval_upper = 120

timeline_limit = 10 #40 is the maximum

from mastodon import Mastodon
import pandas as pd
import mysql.connector
from datetime import datetime
import random
import time
import os

# MySQL connection details
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
database = os.getenv("DB_DATABASE")
username = os.getenv("DB_USERNAME")
password = os.getenv("DB_PASSWORD")

run_till = round(datetime.now().timestamp()) + 60*minutes

while round(datetime.now().timestamp()) < run_till:
  time.sleep(random.randrange(interval_lower, interval_upper))
  dbconn = mysql.connector.connect(
      host=host,
      port=port,
      user=username,
      password=password,
      database=database
  )
  # load random bot
  user_token = pd.read_sql_query("SELECT token FROM accounts WHERE type = 'bot' ORDER BY RAND() LIMIT 1", dbconn)["token"].values[0]
  dbconn.close()
  mastodon = Mastodon(access_token = user_token, api_base_url = 'https://argyle.systems')
  timeline = mastodon.timeline('home', limit = timeline_limit)
  #len(timeline)
  #add random like
  status_to_like_index = random.randint(0,len(timeline)-1) #need to drop things I've already liked, otherwise this throws an error
  print("Liking status " + str(timeline[status_to_like_index]['id']) + " by " + str(timeline[status_to_like_index]['account']['username']))
  try:
      mastodon.status_favourite(str(timeline[status_to_like_index]['id']))
  except:
      print("Couldn't like this status.  Maybe already liked it.")
  #add random reblog some of the time
  if random.uniform(0, 8)<.5:
      status_to_reblog_index = random.randint(0,len(timeline)-1)
      print("Reblogging status " + str(timeline[status_to_reblog_index]['id']) + " by " + str(timeline[status_to_reblog_index]['account']['username']))
      try:
          mastodon.status_reblog(str(timeline[status_to_reblog_index]['id']))
      except:
          print("Couldn't reblog this status.  Maybe already reblogged it.")
  # #add random reply
  # status_to_reply_index = random.randint(0,len(timeline)-1)
  # print("Replying status " + str(timeline[status_to_reply_index]['id']) + " by " + str(timeline[status_to_reply_index]['account']['username']))
  # mastodon.status_post(status = 'Bot reply', in_reply_to_id = str(timeline[status_to_reply_index]['id']))


