import mysql.connector
import requests
import pandas as pd
import time
#from datetime import datetime
import os

# for rss:
import feedparser
import datetime
import pytz
import dateutil.parser
import hashlib

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

# Retrieve rss sources info from the "rss_sources" table
rss_sources = pd.read_sql_query('SELECT * FROM rss_sources', dbconn)

# Create table if it doesn't exist
create_table_query = """
    CREATE TABLE IF NOT EXISTS rss_content (
        rss_post_id CHAR(255) PRIMARY KEY,
        rss_source_id TINYTEXT,
        url TINYTEXT,
        title TINYTEXT,
        summary MEDIUMTEXT,
        published_at DATETIME
    )
"""
cursor.execute(create_table_query)

####### new rss functions:
# convert datetime
def struct_time_to_datetime(struct_time):
    return datetime.datetime(*struct_time[:6])

# Create a unique and chronologically ordered ID
def create_unique_id(row, datetime_col, url_col):
    # Convert datetime to a UNIX timestamp in milliseconds
    timestamp = int(row[datetime_col].timestamp() * 1000)
    # Create a short hash of the URL for uniqueness
    hash_obj = hashlib.sha256(row[url_col].encode())
    url_hash = int(hash_obj.hexdigest(), 16) % (10**5)  # Use only the last 5 digits for brevity
    # Combine the timestamp and URL hash
    return f"{timestamp}{url_hash:05d}"


for index, row in rss_sources.iterrows():
    print(f"Processing rss: {row['url']}")
    feed = feedparser.parse(row['url'])
    feed_df = pd.DataFrame(feed.entries)
    if row['time_format']==1:
      feed_df['published_at'] = feed_df['published_parsed'].apply(struct_time_to_datetime)# update this to use row['']
    #columns_to_keep = ['link', 'title', 'summary', 'published_at']
    columns_to_keep = [row['url_name'], row['title_name'], row['summary_name'], 'published_at']
    feed_df = feed_df[columns_to_keep]
    feed_df.columns = ['url', 'title', 'summary', 'published_at'] #rename columns
    feed_df.sort_values('published_at', inplace=True)
    feed_df.insert(0, 'rss_source_id', row['rss_source_id'])
    feed_df.insert(0, 'rss_post_id', feed_df.apply(lambda row: create_unique_id(row, 'published_at', 'url'), axis=1))
    # Filter out posts already in the database
    existing_posts = pd.read_sql_query("SELECT rss_post_id FROM rss_content", dbconn)["rss_post_id"].values
    posts = feed_df[~feed_df['rss_post_id'].isin(existing_posts)]
    #posts = [post for post in posts if post[1] not in existing_posts]
    # Print the length of the posts list
    print(f"Number of posts to be inserted from rss feed {row['publication']} {row['section']}: {len(posts)}")
    # Insert new posts into the database
    if len(posts) > 0:
        try:
            for _, postrow in posts.iterrows():
                insert_query = "INSERT INTO rss_content (rss_post_id, rss_source_id, url, title, summary, published_at) VALUES (%s, %s, %s, %s, %s, %s)"
                values = tuple(postrow)
                cursor.execute(insert_query, values)
                dbconn.commit()
            print(f"Inserted {len(posts)} posts from rss feed {row['publication']} {row['section']}")
        except Exception as e:
            print(f"Error occurred while inserting posts from rss feed {row['publication']} {row['section']}")


# Close the cursor and connection
cursor.close()
dbconn.close()
