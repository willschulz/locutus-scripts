# ~/miniconda3/envs/locutus/bin/python ~/scripts/reddit_bot.py

# bot posting script -- looped version -- works!

from mastodon import Mastodon
import pandas as pd
import mysql.connector
from datetime import datetime
import random
import time
import requests
import os
import subprocess

print("Posting session started at: " + str(datetime.now()))

# MySQL connection details
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
database = os.getenv("DB_DATABASE")
username = os.getenv("DB_USERNAME")
password = os.getenv("DB_PASSWORD")

import traceback

dbconn = mysql.connector.connect(
      host=host,
      port=port,
      user=username,
      password=password,
      database=database
      )

replay_starttime = 1691553600 #aug 9 midnight

rounded_now = int(round(datetime.now().timestamp(), -1))
playback_position = (rounded_now - replay_starttime)



window_secs = 60

#to_toot = pd.read_sql_query('SELECT * FROM scripts where "posted_yet" = 0 AND "scheduled_for" < ' + str(playback_position + window_secs) + ' ORDER BY "scheduled_for" ASC;', dbconn)
#update_query = 'UPDATE scripts SET "posted_yet" = 1 WHERE "posted_yet" = 0 AND "scheduled_for" < ' + str(playback_position + window_secs)

to_toot = pd.read_sql_query('SELECT * FROM scripts WHERE "posted_yet" = 0 AND "scheduled_for" >= ' + str(playback_position) + ' AND "scheduled_for" < ' + str(playback_position + window_secs) + ' ORDER BY "scheduled_for" ASC;', dbconn)
update_query = 'UPDATE scripts SET "posted_yet" = 1 WHERE "posted_yet" = 0 AND "scheduled_for" >= ' + str(playback_position) + ' AND "scheduled_for" < ' + str(playback_position + window_secs) + ';'
cursor = dbconn.cursor()
cursor.execute(update_query)
dbconn.commit()
cursor.close()

#print(to_toot['created_at'])
print(str(to_toot.shape[0]) + " toots to be tooted!")

if to_toot.shape[0]>0:
    for index, row in to_toot.iterrows():
        if 'posted_status' in locals():
            del posted_status
        #time.sleep((row['created_at'] - pd.Timestamp.fromtimestamp((playback_position - window_secs))).total_seconds()) #this caused weird problems
        wait_time = row['scheduled_for'] - (int(datetime.now().timestamp()) - replay_starttime)
        if wait_time>0:
          time.sleep(wait_time)
        print(row['user_id'])
        user_token = pd.read_sql_query("SELECT token FROM accounts WHERE script_user_id = '" + str(row['user_id']) + "'", dbconn)["token"].values[0]
        mastodon = Mastodon(access_token = user_token, api_base_url = 'https://argyle.systems')
        print(row['text'])
        posted_status = mastodon.status_post(row['text'].replace("&amp;", "&").replace("&gt;", ">").replace("&lt;", "<"))
        cursor = dbconn.cursor()
        sql_insert = "INSERT INTO bot_posted(bpid, content, account_id, ideo) VALUES (%s, %s, %s, %s)"
        cursor.execute(sql_insert, (posted_status['id'], posted_status['content'], posted_status['account']['id'], row['ideo']))
        dbconn.commit()
        cursor.close()
        # Fire and forget some_other_script.py with user_id argument
        current_directory = os.path.dirname(os.path.abspath(__file__))
        print("Current directory: " + str(current_directory))
        engagement_script_path = os.path.join(current_directory, 'trailing_engagement_bots.py')
        # subprocess.Popen(['python', engagement_script_path, str(user_token)])#need to figure out path
        log_path = os.path.join(current_directory, 'logs/trailing_engagement_bots.log')
        python_path = os.path.expanduser('~/miniconda3/envs/locutus/bin/python')
        with open(log_path, 'a') as log_file:  # 'a' means append mode, so it won't overwrite the existing logs
          subprocess.Popen([python_path, engagement_script_path, str(user_token)], stdout=log_file, stderr=subprocess.STDOUT)


dbconn.close()
