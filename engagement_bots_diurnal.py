# ~/miniconda3/envs/locutus/bin/python ~/scripts/engagement_bots.py

event_weight = .001 #this is a good event weight
#event_weight = .01 #for testing

like_bias = .75
nonlib_like_prob = 1

minutes = 4 #make 59 in actual implementation

interval_lower = 30
interval_upper = 120

timeline_limit = 40 #40 is the maximum

from mastodon import Mastodon
import pandas as pd
import mysql.connector
from datetime import datetime
import random
import time
import os

start_time = time.time()

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
    if 'ids' in locals():
      del ids
    if 'ids_string' in locals():
      del ids_string
    if 'bot_posted_lookup' in locals():
      del bot_posted_lookup
    if 'lib_indices' in locals():
      del lib_indices
    if 'nonlib_indices' in locals():
      del nonlib_indices
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
    # load random bot
    user_token = pd.read_sql_query("SELECT token FROM accounts WHERE script_user_id IS NOT NULL ORDER BY RAND() LIMIT 1", dbconn)["token"].values[0]
    mastodon = Mastodon(access_token = user_token, api_base_url = 'https://argyle.systems')
    #timeline = mastodon.timeline('home', limit = timeline_limit)
    timeline = mastodon.timeline('local', limit = timeline_limit)
    
    #add random like
    
    texts = [d['content'] for d in timeline]
    ids = [d['id'] for d in timeline]
    ids_string = ', '.join(map(str, ids))
    # Fetch records from bot_posted that match the IDs
    records = pd.read_sql_query(f"SELECT bpid, ideo FROM bot_posted WHERE bpid IN ({ids_string})", dbconn)
    # Convert records into a dictionary
    bpid_ideo_map = dict(zip(records['bpid'], records['ideo']))
    # Build the bot_posted_lookup list
    bot_posted_lookup = [bpid_ideo_map.get(i, "human") for i in ids]
    dbconn.close()
    
    lib_indices = [i for i, s in enumerate(bot_posted_lookup) if s == "lib"]
    # Exclude "human" cases from nonlib_indices
    nonlib_indices = [i for i, s in enumerate(bot_posted_lookup) if s != "lib" and s != "human"]
    
    # ids = [d['id'] for d in timeline]
    # ids_string = ', '.join(map(str, ids))
    # bot_posted_lookup = pd.read_sql_query(f"SELECT ideo FROM bot_posted WHERE bpid IN ({ids_string})", dbconn)['ideo'].tolist()
    # # need to adjust to handle human posts
    # dbconn.close()
    # lib_indices = [i for i, s in enumerate(bot_posted_lookup) if s == "lib"]
    # nonlib_indices = [i for i, s in enumerate(bot_posted_lookup) if s != "lib"]
    
    
    # #original bias code:
    # this_random = random.random()
    # if this_random<like_bias:
    #   print("liking a lib status if possible")
    #   if len(lib_indices)>0:
    #     print("liking a lib status")
    #     status_to_like_index = random.choice(lib_indices)
    #   else:
    #     if len(nonlib_indices)>0:
    #       print("no lib statuses available")
    #       status_to_like_index = random.choice(nonlib_indices)
    # else:
    #   print("liking a nonlib status if possible")
    #   if len(nonlib_indices)>0:
    #     print("liking a nonlib status")
    #     status_to_like_index = random.choice(nonlib_indices)
    #new bias code:
    this_random = random.random()
    if this_random<like_bias:
      print("liking a lib status if possible")
      if len(lib_indices)>0:
        print("liking a lib status")
        status_to_like_index = random.choice(lib_indices)
      else:
        if len(nonlib_indices)>0:
          print("no lib statuses available")
          status_to_like_index = random.choice(nonlib_indices)
    else:
      print("liking a lib status with x prob")
      if random.random()<nonlib_like_prob:
        print("liking a nonlib status if possible")
        if len(nonlib_indices)>0:
          print("liking a nonlib status")
          status_to_like_index = random.choice(nonlib_indices)
        else: 
          print("No nonlib statuses available. Continuing...")
          continue
      else: 
        print("Not liking any statuses due to nonlib_like_prob. Continuing...")
        continue
    #status_to_like_index = random.randint(0,len(timeline)-1) #need to drop things I've already liked, otherwise this throws an error
    print("Liking status " + str(timeline[status_to_like_index]['id']) + "\n" + str(timeline[status_to_like_index]['content']) + "\n" + " by " + str(timeline[status_to_like_index]['account']['username']))
    try:
        mastodon.status_favourite(str(timeline[status_to_like_index]['id']))
    except:
        print("Couldn't like this status.  Maybe already liked it.")
    # #add random reblog some of the time
    # if random.uniform(0, 8)<.5:
    #     status_to_reblog_index = random.randint(0,len(timeline)-1)
    #     print("Reblogging status " + str(timeline[status_to_reblog_index]['id']) + " by " + str(timeline[status_to_reblog_index]['account']['username']))
    #     try:
    #         mastodon.status_reblog(str(timeline[status_to_reblog_index]['id']))
    #     except:
    #         print("Couldn't reblog this status.  Maybe already reblogged it.")
    # #add random reply
    # status_to_reply_index = random.randint(0,len(timeline)-1)
    # print("Replying status " + str(timeline[status_to_reply_index]['id']) + " by " + str(timeline[status_to_reply_index]['account']['username']))
    # mastodon.status_post(status = 'Bot reply', in_reply_to_id = str(timeline[status_to_reply_index]['id']))





# 
# 
# 
# #################
# 
# 
# 
# run_till = round(datetime.now().timestamp()) + 60*minutes
# 
# 
# 
# 
# while round(datetime.now().timestamp()) < run_till:
#   time.sleep(random.randrange(interval_lower, interval_upper))
#   dbconn = mysql.connector.connect(
#       host=host,
#       port=port,
#       user=username,
#       password=password,
#       database=database
#   )
#   # load random bot
#   user_token = pd.read_sql_query("SELECT token FROM accounts WHERE script_user_id IS NOT NULL ORDER BY RAND() LIMIT 1", dbconn)["token"].values[0]
#   dbconn.close()
#   mastodon = Mastodon(access_token = user_token, api_base_url = 'https://argyle.systems')
#   timeline = mastodon.timeline('home', limit = timeline_limit)
#   #len(timeline)
#   #add random like
#   status_to_like_index = random.randint(0,len(timeline)-1) #need to drop things I've already liked, otherwise this throws an error
#   print("Liking status " + str(timeline[status_to_like_index]['id']) + " by " + str(timeline[status_to_like_index]['account']['username']))
#   try:
#       mastodon.status_favourite(str(timeline[status_to_like_index]['id']))
#   except:
#       print("Couldn't like this status.  Maybe already liked it.")
#   #add random reblog some of the time
#   if random.uniform(0, 8)<.5:
#       status_to_reblog_index = random.randint(0,len(timeline)-1)
#       print("Reblogging status " + str(timeline[status_to_reblog_index]['id']) + " by " + str(timeline[status_to_reblog_index]['account']['username']))
#       try:
#           mastodon.status_reblog(str(timeline[status_to_reblog_index]['id']))
#       except:
#           print("Couldn't reblog this status.  Maybe already reblogged it.")
#   # #add random reply
#   # status_to_reply_index = random.randint(0,len(timeline)-1)
#   # print("Replying status " + str(timeline[status_to_reply_index]['id']) + " by " + str(timeline[status_to_reply_index]['account']['username']))
#   # mastodon.status_post(status = 'Bot reply', in_reply_to_id = str(timeline[status_to_reply_index]['id']))
# 

