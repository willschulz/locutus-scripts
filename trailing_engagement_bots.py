min_likes = 2
max_likes = 5

lib_weight = 2
nonlib_weight = 1

timeline_limit = 5 #40 is the maximum, lower limits should even things out faster

from mastodon import Mastodon
import pandas as pd
import mysql.connector
from datetime import datetime
import random
import time
import os
import sys

start_time = time.time()

def weighted_sample_without_replacement(elements, weights, k):
    # Create a list of tuples with element and its weight
    combined = list(zip(elements, weights))
    # Sort the list by weight
    combined.sort(key=lambda x: x[1], reverse=True)
    # Perform weighted sampling without replacement
    result = []
    for _ in range(k):
        cum_weights = [w for e, w in combined]
        total = sum(cum_weights)
        rand_val = random.uniform(0, total)
        for index, weight in enumerate(cum_weights):
            rand_val -= weight
            if rand_val < 0:
                result.append(combined[index][0])
                del combined[index]
                break
    return result

# user_token = "3nGBF2v1PbSEkwPF7hMFlTJ8p57MjUJh5Up8zJ5DUQw"
# 
# type(timeline)
# 
# timeline[1:]

if len(sys.argv) > 1:
    user_token = sys.argv[1]
    import traceback
    num_likes = random.randint(min_likes, max_likes)
    print("Trying to like " + str(num_likes) + " posts...")
    if num_likes > 0: #change this to be number of likes
      #get timeline
      mastodon = Mastodon(access_token = user_token, api_base_url = 'https://argyle.systems')
      timeline = mastodon.timeline('local', limit = timeline_limit + 1)[1:] #get last 5 excluding most recent post (presumptively from the current account)
      
      #identify which are liberal, and which are nonliberal-nonhuman
      ids = [d['id'] for d in timeline]
      ids_string = ', '.join(map(str, ids))
      # Fetch records from bot_posted that match the IDs
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
      records = pd.read_sql_query(f"SELECT bpid, ideo FROM bot_posted WHERE bpid IN ({ids_string})", dbconn)
      # Convert records into a dictionary
      bpid_ideo_map = dict(zip(records['bpid'], records['ideo']))
      # Build the bot_posted_lookup list
      bot_posted_lookup = [bpid_ideo_map.get(i, "human") for i in ids]
      dbconn.close()
      
      lib_indices = [i for i, s in enumerate(bot_posted_lookup) if s == "lib"]
      nonlib_indices = [i for i, s in enumerate(bot_posted_lookup) if s != "lib" and s != "human"]# Exclude "human" cases from nonlib_indices
      pooled_indices = lib_indices + nonlib_indices
      pooled_weights = [lib_weight] * len(lib_indices) + [nonlib_weight] * len(nonlib_indices)
      
      statuses_to_like = weighted_sample_without_replacement(pooled_indices, weights=pooled_weights, k=num_likes)
      for status_to_like_index in statuses_to_like:
        time.sleep(random.randint(1,10))#make adjustable
        print("Liking status " + str(timeline[status_to_like_index]['id']) + "\n" + str(timeline[status_to_like_index]['content']) + "\n" + " by " + str(timeline[status_to_like_index]['account']['username']))
        try:
            mastodon.status_favourite(str(timeline[status_to_like_index]['id']))
        except:
            print("Couldn't like this status.  Maybe already liked it.")



