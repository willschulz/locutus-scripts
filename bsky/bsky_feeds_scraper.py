#WORKING!!!!!!!
import mysql.connector
import time
#import requests
import pandas as pd
#import time
import os

import datetime
#import pytz
#import dateutil.parser
#import hashlib

import numpy as np

 ########## bsky-specifif stuff, creds ##########
from atproto import Client

client = Client(base_url='https://bsky.social')
client.login(os.getenv("BSKY_EMAIL"), os.getenv("BSKY_PASSWORD"))

######## ######## ######## ######## ########

#print('Started scraping rss feeds at ' + str(datetime.now()))

# MySQL connection details
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
database = os.getenv("DB_DATABASE")
username = os.getenv("DB_USERNAME")
password = os.getenv("DB_PASSWORD")

# Connect to MySQL database
dbconn = mysql.connector.connect(
    host=host,
    port=port,
    user=username,
    password=password,
    database=database
)

cursor = dbconn.cursor()

# continue editing from here
# Retrieve bsky feed info from the "bsky_feeds" table
bsky_feeds = pd.read_sql_query('SELECT * FROM bsky_feeds WHERE scrapeable = 1', dbconn)

# Create table if it doesn't exist
create_table_query = """
    CREATE TABLE IF NOT EXISTS bsky_posts (
        post_uri TINYTEXT,
        post_cid TINYTEXT,
        post_created_at DATETIME,
        post_text MEDIUMTEXT,
        post_indexed_at DATETIME,
        author_did TINYTEXT,
        author_handle TINYTEXT,
        author_display_name TINYTEXT,
        author_created_at DATETIME,
        author_avatar TINYTEXT,
        like_count MEDIUMINT,
        repost_count MEDIUMINT,
        reply_count MEDIUMINT,
        quote_count MEDIUMINT,
        viewer_liked TINYINT,
        viewer_reposted TINYINT,
        viewer_thread_muted TINYINT,
        embed_type TINYTEXT,
        embed_external_uri TINYTEXT,
        embed_record_uri TINYTEXT,
        embed_image_uri TINYTEXT,
        embed_count TINYINT,
        PRIMARY KEY (post_cid(59))
    )
"""

cursor.execute(create_table_query)

####### new rss functions:
# convert datetime
# def struct_time_to_datetime(struct_time):
#     return datetime.datetime(*struct_time[:6])

# Create a unique and chronologically ordered ID
def create_unique_id(row, datetime_col, url_col):
    # Convert datetime to a UNIX timestamp in milliseconds
    timestamp = int(row[datetime_col].timestamp() * 1000)
    # Create a short hash of the URL for uniqueness
    hash_obj = hashlib.sha256(row[url_col].encode())
    url_hash = int(hash_obj.hexdigest(), 16) % (10**5)  # Use only the last 5 digits for brevity
    # Combine the timestamp and URL hash
    return f"{timestamp}{url_hash:05d}"

#randomly scramble order of the rows in bsky_feed based on the current system time
bsky_feeds = bsky_feeds.sample(frac=1, random_state=int(time.time()))

for index, row in bsky_feeds.iterrows():
    print(f"Scraping : {row['feed_name']}")
    data = client.app.bsky.feed.get_feed({
        'feed': row['feed_at'],
        #'feed': 'at://did:plc:z72i7hdynmk6r22z27h6tvur/app.bsky.feed.generator/whats-hot',
        'limit': 30,
    }, headers={'Accept-Language': 'en'})
    feed = data.feed
    df = parse_feedviewpost_list(feed)
    #report posts scraped
    print(f"Scraped {len(df)} posts from bsky feed {row['feed_name']}")
    if len(df) > 0:
        # filter out posts that we already have
        existing_posts = pd.read_sql_query("SELECT post_cid FROM bsky_posts", dbconn)["post_cid"].values
        df = df[~df['post_cid'].isin(existing_posts)]
        #report number of new posts
        print(f"Found {len(df)} new posts from bsky feed {row['feed_name']}")
        if len(df) > 0:
            try:
              values = [tuple(row) for row in df.itertuples(index=False, name=None)]
              insert_query = f"INSERT INTO bsky_posts ({', '.join(df.columns)}) VALUES ({', '.join(['%s'] * len(df.columns))})"
              cursor.executemany(insert_query, values)
              dbconn.commit()  # Commit changes for the current subreddit
              print(f"Inserted {len(df)} posts from bsky feed {row['feed_name']}")
            except Exception as e:
              print(f"Error occurred while inserting posts from bsky feed {row['feed_name']}")
    time.sleep(1)


# Close the cursor and connection
cursor.close()
dbconn.close()
