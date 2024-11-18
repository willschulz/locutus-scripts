from mastodon import Mastodon
import pandas as pd
#from datetime import datetime
import mysql.connector
#import time
import os

# MySQL connection details
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
database = os.getenv("DB_DATABASE")
username = os.getenv("DB_USERNAME")
password = os.getenv("DB_PASSWORD")


import re

dbconn = mysql.connector.connect(
    host=host,
    port=port,
    user=username,
    password=password,
    database=database
)

# Initiate scraping session with token from random user -- probably should use my admin account when I figure out how to remove all rate limits
user_token = pd.read_sql_query("SELECT token FROM accounts WHERE aid = 'moth.beta.argyle.social'", dbconn)["token"].values[0]
mastodon = Mastodon(access_token = user_token, api_base_url = 'https://argyle.systems')
t = mastodon.timeline('local', limit = 40)

#all_mastodon_ids_in_db = pd.read_sql_query("SELECT id FROM mirror_accounts", dbconn).values.tolist()
#all_mastodon_ids_in_db = [item for sublist in all_mastodon_ids_in_db for item in sublist]
#filtered_posts = [post for post in t if post['account']['id'] not in all_mastodon_ids_in_db]

t.sort(key=lambda x: x['created_at'], reverse=True)
most_recent_post = t[0] if t else None
post_id_to_reply_to = most_recent_post['id']
account_acct_to_reply_to = most_recent_post['account']['acct']
content_to_reply_to = most_recent_post['content']

text_of_reply = get_openai_response(system_instruction_supportive, content_to_reply_to).choices[0].message.content

posted_status = mastodon.status_post("@" + str(account_acct_to_reply_to) + " " + text_of_reply, in_reply_to_id = post_id_to_reply_to)

dbconn.close()
