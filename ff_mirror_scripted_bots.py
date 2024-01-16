
from mastodon import Mastodon
import pandas as pd
import mysql.connector
from datetime import datetime
import random
import time
import requests
import os
import subprocess

#import multiprocessing #try to make posting simultaneous later--rn it's close enough

print("Posting session started at: " + str(datetime.now()))

#MySQL connection details
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

to_toot = pd.read_sql_query('SELECT * FROM mirror_scripts ORDER BY "scheduled_for" ASC;', dbconn)

print(str(to_toot.shape[0]) + " toots to be tooted!")

for index, row in to_toot.iterrows():
    time.sleep(10)
    these_accounts = pd.read_sql_query("SELECT * FROM mirror_accounts WHERE script_user_id = '" + str(row['user_id']) + "'", dbconn)        
    for account_index, account_row in these_accounts.iterrows(): #todo: inititiate posting as separate processes
      mastodon = Mastodon(access_token = account_row["token"], api_base_url = account_row["instance_base_url"])
      if row['reply_to_pseudoid'] is not None:
        reply_to_status_id = pd.read_sql_query("SELECT bpid FROM mirror_bot_posted WHERE reply_pseudoid = '" + str(row['reply_to_pseudoid']) + "' AND instance_base_url = '" + str(account_row['instance_base_url']) + "' ORDER BY bpid DESC LIMIT 1;", dbconn)['bpid'].values[0]
        posted_status = mastodon.status_post(row['text'].replace("&amp;", "&").replace("&gt;", ">").replace("&lt;", "<"), in_reply_to_id = reply_to_status_id)
      else:
        posted_status = mastodon.status_post(row['text'].replace("&amp;", "&").replace("&gt;", ">").replace("&lt;", "<"))
      cursor = dbconn.cursor()
      if row['reply_pseudoid'] is not None:
        sql_insert = "INSERT INTO mirror_bot_posted(bpid, content, account_id, ideo, reply_pseudoid, instance_base_url) VALUES (%s, %s, %s, %s, %s, %s)"
        cursor.execute(sql_insert, (posted_status['id'], posted_status['content'], posted_status['account']['id'], row['ideo'], row['reply_pseudoid'], account_row['instance_base_url']))
      else:
        sql_insert = "INSERT INTO mirror_bot_posted(bpid, content, account_id, ideo, instance_base_url) VALUES (%s, %s, %s, %s, %s)"
        cursor.execute(sql_insert, (posted_status['id'], posted_status['content'], posted_status['account']['id'], row['ideo'], account_row['instance_base_url']))
      dbconn.commit()
      cursor.close()
      current_directory = os.path.dirname(os.path.abspath(__file__))
      engagement_script_path = os.path.join(current_directory, 'mirror_likebots.py')
      log_path = os.path.join(current_directory, 'logs/mirror_likebots.log') #todo: log separately for each verse?
      python_path = os.path.expanduser('~/miniconda3/envs/locutus/bin/python')
      with open(log_path, 'a') as log_file:  # 'a' means append mode, so it won't overwrite the existing logs
        subprocess.Popen([python_path, engagement_script_path, str(posted_status['id']), str(posted_status['account']['id']), str(account_row["instance_base_url"]), str(row['ideo'])], stdout=log_file, stderr=subprocess.STDOUT)


dbconn.close()


