# to do later:
# create table for posted content and check against it when selecting new content

# for a given server or set of servers

instance_base_url = 'https://alpha.argyle.social'

import 

from mastodon import Mastodon
import pandas as pd
import mysql.connector
from datetime import datetime
import random
import time
import requests
import os

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


unposted_post = pd.read_sql_query("SELECT * FROM bsky_posts ORDER BY post_created_at DESC LIMIT 1", dbconn)

#unposted_post


post_did = unposted_post.iloc[0]['author_did']

#check the mirror_accounts table and return a logical for whether or not there is already an account with this instance_base_url and clone_user_id equal to post_did
account_exists = pd.read_sql_query("SELECT * FROM mirror_accounts WHERE instance_base_url = '" +
                                   instance_base_url +
                                   "' AND clone_user_id = '" +
                                   post_did +
                                   "'",
                                   dbconn).shape[0] > 0

account_exists

#account_exists

#if the account doesn't exist, then do stuff
#if not account_exists:
    #create a new account


