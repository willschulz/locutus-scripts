
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

##MySQL connection details
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
database = os.getenv("DB_DATABASE")
username = os.getenv("DB_USERNAME")
password = os.getenv("DB_PASSWORD")

SPACES_KEY = os.getenv("SPACES_KEY")
SPACES_SECRET = os.getenv("SPACES_SECRET")

import traceback


dbconn = mysql.connector.connect(
      host=host,
      port=port,
      user=username,
      password=password,
      database=database
      )

time_dilation = 3
replay_starttime = 1715902709-28149
rounded_now = int(round(datetime.now().timestamp(), -1))
playback_position = (rounded_now - replay_starttime)*time_dilation

#remember to comment out below at launch
#playback_position = playback_position - (4*60*60)

window_secs = 60*time_dilation

#to_toot = pd.read_sql_query('SELECT * FROM mirror_scripts WHERE "posted_yet" = 0 AND "scheduled_for" >= ' + str(playback_position) + ' AND "scheduled_for" < ' + str(playback_position + window_secs) + ' ORDER BY "scheduled_for" ASC;', dbconn)
#update_query = 'UPDATE mirror_scripts SET "posted_yet" = 1 WHERE "posted_yet" = 0 AND "scheduled_for" >= ' + str(playback_position) + ' AND "scheduled_for" < ' + str(playback_position + window_secs) + ';'
to_toot = pd.read_sql_query('SELECT * FROM mirror_scripts WHERE "posted_yet" = 0 AND "scheduled_for" < ' + str(playback_position + window_secs) + ' ORDER BY "scheduled_for" ASC;', dbconn)
update_query = 'UPDATE mirror_scripts SET "posted_yet" = 1 WHERE "posted_yet" = 0 AND "scheduled_for" < ' + str(playback_position + window_secs) + ';'
cursor = dbconn.cursor()
cursor.execute(update_query)
dbconn.commit()
cursor.close()

# # for testing:
# playback_position = 217250
# to_toot = pd.read_sql_query('SELECT * FROM mirror_scripts WHERE "scheduled_for" >= ' + str(playback_position) + ' AND "scheduled_for" < ' + str(playback_position + window_secs) + ' ORDER BY "scheduled_for" ASC;', dbconn)
# window_secs = 60*5

print("Playback position: " + str(playback_position))
print(str(to_toot.shape[0]) + " toots to be tooted!")


### new ###
import re
import os
import pandas as pd
import boto3
from urllib.parse import urlparse

s3 = boto3.client(
        's3',
        region_name='nyc3',  # or other regions
        endpoint_url='https://nyc3.digitaloceanspaces.com',
        aws_access_key_id=SPACES_KEY,
        aws_secret_access_key=SPACES_SECRET
    )

if to_toot.shape[0]>0:
    for index, row in to_toot.iterrows():
        if 'posted_status' in locals():
            del posted_status
        #time.sleep(10) #for testing
        wait_time = row['scheduled_for'] - (int(datetime.now().timestamp()) - replay_starttime)*time_dilation
        if wait_time>0:
          time.sleep(wait_time)
        these_accounts = pd.read_sql_query("SELECT * FROM mirror_accounts WHERE script_user_id = '" + str(row['user_id']) + "'", dbconn)        
        for account_index, account_row in these_accounts.iterrows(): #todo: inititiate posting as separate processes
          mastodon = Mastodon(access_token = account_row["token"], api_base_url = account_row["instance_base_url"])
          if row['reply_to_pseudoid'] is not None:
            reply_to_status_id = pd.read_sql_query("SELECT bpid FROM mirror_bot_posted WHERE reply_pseudoid = '" + str(row['reply_to_pseudoid']) + "' AND instance_base_url = '" + str(account_row['instance_base_url']) + "' ORDER BY bpid DESC LIMIT 1;", dbconn)['bpid'].values[0]
            reply_to_muid = pd.read_sql_query("SELECT account_id FROM mirror_bot_posted WHERE reply_pseudoid = '" + str(row['reply_to_pseudoid']) + "' AND instance_base_url = '" + str(account_row['instance_base_url']) + "' ORDER BY bpid DESC LIMIT 1;", dbconn)['account_id'].values[0]
            reply_to_handle = pd.read_sql_query("SELECT name FROM mirror_accounts WHERE id = '" + str(reply_to_muid) + "' ;", dbconn)['name'].values[0]
            if row['media_url'] is not None:
              response = s3.get_object(Bucket=bucket, Key=object_key)
              content = response['Body'].read()
              media = mastodon.media_post(content, media_type)
              posted_status = mastodon.status_post("@" + str(reply_to_handle) + " " + row['text'].replace("&amp;", "&").replace("&gt;", ">").replace("&lt;", "<"), in_reply_to_id = reply_to_status_id, media_ids=media)
            else:
              posted_status = mastodon.status_post("@" + str(reply_to_handle) + " " + row['text'].replace("&amp;", "&").replace("&gt;", ">").replace("&lt;", "<"), in_reply_to_id = reply_to_status_id)
          else:
            if row['media_url'] is not None:
              #media_url = "https://nyc3.digitaloceanspaces.com/files.azx.argyle.systems/media_attachments/files/110/945/668/446/777/463/original/331bcb3feae74520.jpg"
              media_url = str(row['media_url'])
              # Extract the bucket name from the URL
              bucket = re.sub(r"https://nyc3.digitaloceanspaces.com/", "", media_url)
              bucket = re.sub(r"/.*", "", bucket)
              # Extract the object key from the URL
              object_key = re.sub(r"https://nyc3.digitaloceanspaces.com/", "", media_url)
              object_key = re.sub(rf"{bucket}/", "", object_key)
              response = s3.get_object(Bucket=bucket, Key=object_key)
              content = response['Body'].read()
              media_type = "image/jpeg"# todo: make flexible
              media = mastodon.media_post(content, media_type)
              posted_status = mastodon.status_post(row['text'].replace("&amp;", "&").replace("&gt;", ">").replace("&lt;", "<"), media_ids=media)
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
