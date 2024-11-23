# ~/miniconda3/envs/locutus/bin/python ~/ops/230711_init/scripts_dev/reddit_scraper.py
# ~/miniconda3/envs/locutus/bin/python ~/scripts/reddit_scraper.py

statuses_per_scrape = 5
listing = "new"
timeframe = "day"

import mysql.connector
import requests
import pandas as pd
import time
from datetime import datetime

print('Started scraping reddit at ' + str(datetime.now()))

import os
print(os.getcwd())
os.environ['TESSDATA_PREFIX'] = "miniconda3/envs/locutus/share/tessdata"

#for OCR:
import pytesseract
from PIL import Image, ImageOps
from io import BytesIO

# # MySQL connection details
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
database = os.getenv("DB_DATABASE")
username = os.getenv("DB_USERNAME")
password = os.getenv("DB_PASSWORD")

# Function to get Reddit data
def get_reddit(subreddit, listing, limit, timeframe):
    try:
        base_url = f"https://www.reddit.com/r/{subreddit}/{listing}.json?limit={limit}&t={timeframe}"
        request = requests.get(base_url, headers={"User-agent": "yourbot"})
    except:
        print("An Error Occurred")
    return request.json()

# Function to get post titles, links, and media
def get_post_titles(r):
    posts = []
    for post in r["data"]["children"]:
        title = post["data"]["title"]
        link = post["data"]["url"]
        permalink = post["data"]["permalink"]
        subreddit_id = post["data"]["subreddit_id"]
        post_id = f"{subreddit_id}_{post['data']['id']}_{post['data']['created_utc']}"
        posts.append((subreddit_id, post_id, post["data"]["created_utc"], title, link, permalink))
    return posts

# Connect to MySQL database
dbconn = mysql.connector.connect(
    host=host,
    port=port,
    user=username,
    password=password,
    database=database
)


# Retrieve subreddit names from the "sources" table
query = "SELECT name FROM sources WHERE source = 'reddit'"
cursor = dbconn.cursor()
cursor.execute(query)
subreddits = cursor.fetchall()
subreddits = [subreddit[0] for subreddit in subreddits]

# Create table if it doesn't exist
create_table_query = """
    CREATE TABLE IF NOT EXISTS reddit_scrapes (
        subreddit_id VARCHAR(255),
        post_id VARCHAR(255) PRIMARY KEY,
        created_at_utc BIGINT,
        title TEXT,
        link TEXT,
        permalink TEXT,
        ocr_text TEXT,
        bot_posted TINYINT(2)
    )
"""
cursor.execute(create_table_query)

# Iterate over the subreddits and scrape data
for subreddit in subreddits:
    print(f"Processing subreddit: {subreddit}")
    reddit_scrape_output = get_reddit(subreddit, listing, statuses_per_scrape, timeframe)
    posts = get_post_titles(reddit_scrape_output)
    #posts = ocr_images(posts)
    # Filter out posts already in the database
    existing_posts = pd.read_sql_query("SELECT post_id FROM reddit_scrapes", dbconn)["post_id"].values
    posts = [post for post in posts if post[1] not in existing_posts]
    # # Print the posts to be inserted
    # print(f"Posts to be inserted for subreddit {subreddit}:")
    # for post in posts:
    #     print(post)
    # Print the length of the posts list
    print(f"Number of posts to be inserted for subreddit {subreddit}: {len(posts)}")
    # Insert new posts into the database
    if len(posts) > 0:
        insert_query = "INSERT INTO reddit_scrapes (subreddit_name, subreddit_id, post_id, created_at_utc, title, link, permalink, bot_posted) VALUES ('"+subreddit+"', %s, %s, %s, %s, %s, %s, 20)"
        try:
            cursor.executemany(insert_query, posts)
            dbconn.commit()  # Commit changes for the current subreddit
            print(f"Inserted {len(posts)} posts for subreddit {subreddit}")
        except Exception as e:
            print(f"Error occurred while inserting posts for subreddit {subreddit}: {str(e)}")

# Close the cursor and connection
cursor.close()
dbconn.close()

