base_likes = 1
min_extralikes = 0
max_extralikes = 2

from mastodon import Mastodon
import pandas as pd
import mysql.connector
from datetime import datetime
import random
import time
import os
import sys
import traceback

start_time = time.time()

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
  database=database)

post_bpid = sys.argv[1]
post_muid = sys.argv[2]
instance_base_url = sys.argv[3]
post_ideo = sys.argv[4]

biases = pd.read_sql_query("SELECT lib_weight, mod_weight, non_weight, pop_weight FROM mirror_verses WHERE instance_base_url = '" + instance_base_url + "'", dbconn)
lib_weight = biases['lib_weight'][0]
mod_weight = biases['mod_weight'][0]
non_weight = biases['non_weight'][0]
pop_weight = biases['pop_weight'][0]

# determine how many bots will like this post
if post_ideo == "lib":
  if lib_weight>1:
     num_likes = base_likes*lib_weight + random.randint(min_extralikes, max_extralikes)
  else:
     num_likes = base_likes*lib_weight
elif post_ideo == "mod":
  if lib_weight>1:
     num_likes = base_likes*mod_weight + random.randint(min_extralikes, max_extralikes)
  else:
     num_likes = base_likes*mod_weight
elif post_ideo == "non":
  num_likes = base_likes*non_weight# + random.randint(min_extralikes, max_extralikes)
elif post_ideo == "pop":
  num_likes = base_likes*pop_weight# + random.randint(min_extralikes, max_extralikes)
else:
  num_likes = 0

print("Giving " + str(num_likes) + " likes...")
#total_time = random.randint(2,3)
total_time = (5 + (random.random()*2))*60 #5-7 minutes
if num_likes > 0: #change this to be number of likes
  liker_tokens = pd.read_sql_query("SELECT token FROM mirror_accounts WHERE type = 'bot' AND id != '" + str(post_muid) + "' AND instance_base_url = '" + str(instance_base_url) + "' ORDER BY RAND() LIMIT " + str(num_likes) + ";", dbconn)
  dbconn.close()
  for index, row in liker_tokens.iterrows():
    time.sleep(total_time/num_likes)
    print(row['token'])
    print(instance_base_url)
    mastodon = Mastodon(access_token = str(row['token']), api_base_url = instance_base_url)
    print("Liking status " + str(post_bpid))
    try:
        mastodon.status_favourite(str(post_bpid))
    except:
        print("Couldn't like this status.")

