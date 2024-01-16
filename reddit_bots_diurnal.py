# ~/miniconda3/envs/locutus/bin/python ~/scripts/reddit_bot.py

event_weight = .0001

apol_rate = .01

print("Starting...")

minutes = 5 #make 59 in actual implementation


replay_starttime = 1691553600 #-- remember to update when relaunching scripts!
window_secs = 60*30


from mastodon import Mastodon
import pandas as pd
import mysql.connector
from datetime import datetime
import random
import time
import requests
import os


#for diurnal probability function
import numpy as np

def generate_diurnal_probability(current_time, amplitude=0.5, phase=0, offset=0.5):
    # Calculate the minute_of_day from the current_time
    minute_of_day = current_time.hour * 60 + current_time.minute
    
    # Convert minute_of_day to radians (period of 24 hours)
    radian = 2 * np.pi * minute_of_day / (24 * 60)
    
    # Calculate the probability using a sine function
    probability = amplitude * np.sin(radian + phase) + offset
    
    # Ensure probability is within [0, 1] range
    probability = np.clip(probability, 0, 1)
    
    return probability

# Get the probability of action at current time
#generate_diurnal_probability(datetime.now())



# MySQL connection details
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
database = os.getenv("DB_DATABASE")
username = os.getenv("DB_USERNAME")
password = os.getenv("DB_PASSWORD")


print("Checking bot count...")

dbconn = mysql.connector.connect(
              host=host,
              port=port,
              user=username,
              password=password,
              database=database
          )
#bot_count = pd.read_sql_query("SELECT COUNT(*) AS bot_count FROM accounts WHERE type = 'bot'", dbconn)['bot_count'][0] #adjust to subset to bots with active scripts?
bot_count = pd.read_sql_query("SELECT COUNT(*) AS bot_count FROM accounts WHERE script_user_id IS NOT NULL", dbconn)['bot_count'][0] #adjust to subset to bots with active scripts?

dbconn.close()

print("There are " + str(bot_count) + " bots...")

run_till = round(datetime.now().timestamp()) + 60*minutes
import traceback
while round(datetime.now().timestamp()) < run_till:
  time.sleep(1)
  if generate_diurnal_probability(datetime.now(), phase = 3.2, offset = .4, amplitude = .75)*event_weight*bot_count > random.random():
    print("Diurnal probabilistic event occurred!")
    print('At datetime: ' + str(datetime.now()))
    print("Probability of event: " + str(generate_diurnal_probability(datetime.now(), phase = 3.2, offset = .4, amplitude = .75)*event_weight*bot_count))
    dbconn = mysql.connector.connect(
              host=host,
              port=port,
              user=username,
              password=password,
              database=database
          )
    #identify users to exclude
    #get recent posts
    user_token = pd.read_sql_query("SELECT token FROM accounts WHERE script_user_id IS NOT NULL ORDER BY RAND() LIMIT 1", dbconn)["token"].values[0]
    mastodon = Mastodon(access_token = user_token, api_base_url = 'https://argyle.systems')
    timeline = mastodon.timeline('local', limit = 40)
    recently_posted_uids = [d['account']['id'] for d in timeline]
    # get nearby scripted posts
    rounded_now = int(round(datetime.now().timestamp(), -1))
    playback_position = (rounded_now - replay_starttime)
    nearby_scripted_uids = pd.read_sql_query('SELECT user_id FROM scripts WHERE "scheduled_for" > ' + str(playback_position - window_secs) + ' AND "scheduled_for" < ' + str(playback_position + window_secs) + ' ORDER BY "scheduled_for" ASC;', dbconn)['user_id'].tolist()
    #merge
    uids_to_exclude = recently_posted_uids + nearby_scripted_uids
    uids_to_exclude_string = ', '.join(map(str, uids_to_exclude))
    #exclude from candidacy for current posting
    current_user = pd.read_sql_query(f"SELECT * FROM accounts WHERE script_user_id IS NOT NULL AND id NOT IN ({uids_to_exclude_string}) ORDER BY RAND() LIMIT 1", dbconn) #prevent repeat posting and near-future scripted posting
    dbconn.close()
    #to do: add variation in users' propensity to post news / memes
    #for now just repost some reddit thing randomly
    #to do: add filter to rule out users who just posted/are about to post by checking script
    try:
        #time.sleep(random.randrange(1, 5))
        posted_yet = False
        while not posted_yet:
          if 'posted_status' in locals():
            del posted_status
          if 'current_ideo' in locals():
            del current_ideo
          dbconn = mysql.connector.connect(
              host=host,
              port=port,
              user=username,
              password=password,
              database=database
          )
          #choose post type based on account info
          if current_user['apol_memes'][0]==1:
            if random.random()>apol_rate:
              unposted_post = pd.read_sql_query("SELECT * FROM reddit_scrapes WHERE bot_posted = 32 ORDER BY created_at_utc DESC LIMIT 1", dbconn)
              current_ideo = 'apol'
            else:
              print("Skipping apol meme to balance...")
              continue
          elif (current_user['lib_memes'][0]==1) & (current_user['lib_news'][0]==0):
            unposted_post = pd.read_sql_query("SELECT * FROM reddit_scrapes WHERE bot_posted = 33 ORDER BY created_at_utc DESC LIMIT 1", dbconn)
            current_ideo = 'lib'
          elif (current_user['lib_memes'][0]==0) & (current_user['lib_news'][0]==1):
            unposted_post = pd.read_sql_query("SELECT * FROM reddit_scrapes WHERE bot_posted = 34 ORDER BY created_at_utc DESC LIMIT 1", dbconn)
            current_ideo = 'lib'
          elif (current_user['lib_memes'][0]==1) & (current_user['lib_news'][0]==1):
            current_ideo = 'lib'
            if random.random() > .5:
              unposted_post = pd.read_sql_query("SELECT * FROM reddit_scrapes WHERE bot_posted = 33 ORDER BY created_at_utc DESC LIMIT 1", dbconn)
            else:
              unposted_post = pd.read_sql_query("SELECT * FROM reddit_scrapes WHERE bot_posted = 34 ORDER BY created_at_utc DESC LIMIT 1", dbconn)
          else:
            print("This account doesn't post live content...")
            posted_yet = True
            continue
          #unposted_post = pd.read_sql_query("SELECT * FROM reddit_scrapes AS rs INNER JOIN sources AS s ON rs.subreddit_name = s.name WHERE bot_posted = 0 AND s.flavor = '" + current_user['flavor'][0] + "' ORDER BY created_at_utc DESC LIMIT 1", dbconn)
          if unposted_post.empty:
            print("No new posts...")
            #time.sleep(random.randrange(5, 20))
            posted_yet = True #stop trying to post from this user if there are no new posts compatible with their flavor
            continue
          #censoring
          print(unposted_post['title'][0])
          print("Attempting to post " + unposted_post['post_id'].values[0] + " at " + str(datetime.now()))
          mastodon = Mastodon(access_token=current_user['token'][0], api_base_url='https://argyle.systems')
          title = unposted_post['title'][0].replace("&amp;", "&") #fix "&amp;"
          link = unposted_post['link'][0]
          permalink = unposted_post['permalink'][0]
          if link.endswith('.jpg') or link.endswith('.png'):
              media_type = "image/jpeg" if link.endswith('.jpg') else "image/png"
              media = mastodon.media_post(requests.get(link).content, media_type)
              posted_status = mastodon.status_post(title, media_ids=media)
          else:
              title = title + "\n" + link #make a version that allows for posting without a link?
              posted_status = mastodon.status_post(title)
          # mark post as posted
          query = "UPDATE reddit_scrapes SET bot_posted = 1 WHERE post_id = '" + unposted_post['post_id'][0] + "'"
          cursor = dbconn.cursor()
          cursor.execute(query)
          dbconn.commit()
          cursor.close()
          #save posted_status info to bot_posted db
          cursor = dbconn.cursor()
          sql_insert = "INSERT INTO bot_posted(bpid, content, account_id, ideo) VALUES (%s, %s, %s, %s)"
          cursor.execute(sql_insert, (posted_status['id'], posted_status['content'], posted_status['account']['id'], current_ideo))
          dbconn.commit()
          cursor.close()
          dbconn.close()
          posted_yet = True
    except Exception as e:
        traceback.print_exc()
        print("Error:", str(e))
        # mark post as bad
        print("Post created some kidn of error.  Marking as bad")
        query = "UPDATE reddit_scrapes SET bot_posted = 99 WHERE post_id = '" + unposted_post['post_id'][0] + "'"
        cursor = dbconn.cursor()
        cursor.execute(query)
        dbconn.commit()
        cursor.close()
        dbconn.close()
        continue
