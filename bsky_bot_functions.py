# to do later:
# create table for posted content and check against it when selecting new content

# for a given server or set of servers

instance_base_url = 'https://alpha.argyle.social'

import py_functions.account_creation

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


#unposted_post = pd.read_sql_query("SELECT * FROM bsky_posts ORDER BY post_created_at DESC LIMIT 1", dbconn)

#make another sql call to get a purely random post
unposted_post = pd.read_sql_query("SELECT * FROM bsky_posts ORDER BY RAND() LIMIT 1", dbconn).iloc[0]

#unposted_post = unposted_post.iloc[0]

post_did = unposted_post['author_did']

#check the mirror_accounts table and return a logical for whether or not there is already an account with this instance_base_url and clone_user_id equal to post_did
account_exists = pd.read_sql_query("SELECT * FROM mirror_accounts WHERE instance_base_url = '" +
                                   instance_base_url +
                                   "' AND clone_user_id = '" +
                                   post_did +
                                   "'",
                                   dbconn).shape[0] > 0

def make_clean_name(name):
    #remove anything following a . or @, including the . or @
    name = name.split('.')[0]
    name = name.split('@')[0]
    return name

#if the account doesn't exist
if not account_exists:
    #create a new account
    #split instance_base_url into subdomain and domain
    subdomain = instance_base_url.split('.')[0]
    #removing 'https://' from the beginning
    subdomain = subdomain.split('//')[1]
    domain = '.'.join(instance_base_url.split('.')[1:])
    py_functions.account_creation.create_account(name = make_clean_name(unposted_post['author_handle']),
                                                subdomain = subdomain,
                                                domain = domain,
                                                type = 'bsky_clone',
                                                clone_user_id = post_did,
                                                all_follow = True,
                                                avatar_image=unposted_post['author_avatar'])

# then post the content via the bot
print(unposted_post['post_text'])
print("Attempting to post " + unposted_post['post_cid'] + " at " + str(datetime.now()))

account_exists = pd.read_sql_query("SELECT * FROM mirror_accounts WHERE instance_base_url = '" +
                                   instance_base_url +
                                   "' AND clone_user_id = '" +
                                   post_did +
                                   "'",
                                   dbconn).shape[0] > 0

current_user = pd.read_sql_query(f"SELECT * FROM mirror_accounts WHERE clone_user_id = '" + post_did + "'", dbconn).iloc[0]
#current_user

mastodon = Mastodon(access_token=current_user['token'], api_base_url=current_user['instance_base_url'])

if unposted_post['embed_external_uri']=='' & unposted_post['embed_image_uri']=='':
    posted_status = mastodon.status_post(unposted_post['post_text'])
# logical to check whether there is an embed_external_uri
if unposted_post['embed_external_uri']!='' & unposted_post['embed_image_uri']=='':
    posted_status = mastodon.status_post(unposted_post['post_text'] + "\n" + unposted_post['embed_external_uri'])

# if link.endswith('.jpg') or link.endswith('.png'):
#     media_type = "image/jpeg" if link.endswith('.jpg') else "image/png"
#     media = mastodon.media_post(requests.get(link).content, media_type)
#     posted_status = mastodon.status_post(title, media_ids=media)
# else:
#     title = title + "\n" + link #make a version that allows for posting without a link?
#     posted_status = mastodon.status_post(title)
# mark post as posted