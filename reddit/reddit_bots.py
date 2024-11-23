# ~/miniconda3/envs/locutus/bin/python ~/scripts/reddit_bot.py

# bot posting script -- looped version -- works!
minutes = 59 #make 59 in actual implementation
similarity_threshold = .9

title_keywords = ["reddit", "mods", "sub ", "subs ", "downvote", "upvote", "karma", "repost", "r/"]
link_keywords = ["reddit.com", "v.redd.it"]
ocr_keywords = ["reddit", "mods", "sub ", "subs ", "downvote", "upvote", "karma", "repost"]

from mastodon import Mastodon
import pandas as pd
import mysql.connector
from datetime import datetime
import random
import time
import requests
import os

#for deduplication
from difflib import SequenceMatcher

def text_similarity(text1, text2):
    sequence_matcher = SequenceMatcher(None, text1, text2)
    similarity_ratio = sequence_matcher.ratio()
    return similarity_ratio

def is_post_too_similar(target_text, df, threshold):
    similar_posts = []
    # Calculate similarity scores between the target text and all elements in the DataFrame column
    df['similarity_score'] = df.iloc[:, 0].apply(lambda x: text_similarity(target_text, x))
    # Check if any similarity score exceeds the threshold
    if df['similarity_score'].max() > threshold:
        # Filter DataFrame to get all posts that exceed the threshold
        similar_posts = df[df['similarity_score'] > threshold].iloc[:, 0].tolist()
    # Print the message if there are similar posts
    if similar_posts:
        print(f"Candidate post was too similar to:\n{', '.join(similar_posts)}")
        return True
    else:
        return False


# MySQL connection details
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
database = os.getenv("DB_DATABASE")
username = os.getenv("DB_USERNAME")
password = os.getenv("DB_PASSWORD")

run_till = round(datetime.now().timestamp()) + 60*minutes

import traceback

while round(datetime.now().timestamp()) < run_till:
    try:
        time.sleep(random.randrange(5, 20))
        dbconn = mysql.connector.connect(
            host=host,
            port=port,
            user=username,
            password=password,
            database=database
        )
        unposted_post = pd.read_sql_query("SELECT * FROM reddit_scrapes WHERE bot_posted = 0 ORDER BY RAND() LIMIT 1", dbconn)
        if unposted_post.empty:
          print("No new posts... sleeping for a bit.")
          time.sleep(random.randrange(5, 20))
          continue
        print(unposted_post['title'][0])
        if any(keyword in unposted_post['title'][0] for keyword in title_keywords) or any(keyword in unposted_post['link'][0] for keyword in link_keywords) or (unposted_post['ocr_text'][0] and any(keyword in unposted_post['ocr_text'][0] for keyword in ocr_keywords)):
            # mark post as censored
            print("Post contained a banned keyword.  Censoring...")
            query = "UPDATE reddit_scrapes SET bot_posted = 97 WHERE post_id = '" + unposted_post['post_id'][0] + "'"
            cursor = dbconn.cursor()
            cursor.execute(query)
            dbconn.commit()
            cursor.close()
            dbconn.close()
            continue
        old_titles = pd.read_sql_query("SELECT title FROM reddit_scrapes WHERE bot_posted = 1 ORDER BY created_at_utc DESC LIMIT 100", dbconn)
        post_too_similar = is_post_too_similar(unposted_post['title'][0], old_titles, similarity_threshold)
        if post_too_similar:
            print("Marking post as duplicate...")
            query = "UPDATE reddit_scrapes SET bot_posted = 98 WHERE post_id = '" + unposted_post['post_id'][0] + "'"
            cursor = dbconn.cursor()
            cursor.execute(query)
            dbconn.commit()
            cursor.close()
            dbconn.close()
            continue
        print("Attempting to post " + unposted_post['post_id'].values[0] + " at " + str(datetime.now()))
        post_flavor = pd.read_sql_query("SELECT flavor FROM sources WHERE name = '" + unposted_post['subreddit_name'].values[0] + "'", dbconn)["flavor"].values[0]
        user_token = pd.read_sql_query("SELECT token FROM accounts WHERE flavor = '" + post_flavor + "' ORDER BY RAND() LIMIT 1", dbconn)["token"].values[0]
        mastodon = Mastodon(access_token=user_token, api_base_url='https://argyle.systems')
        title = unposted_post['title'][0].replace("&amp;", "&") #fix "&amp;"
        link = unposted_post['link'][0]
        permalink = unposted_post['permalink'][0]
        if link.endswith('.jpg') or link.endswith('.png'):
            media_type = "image/jpeg" if link.endswith('.jpg') else "image/png"
            media = mastodon.media_post(requests.get(link).content, media_type)
            mastodon.status_post(title, media_ids=media)
        else:
            title = title + "\n" + link
            mastodon.status_post(title)
        # mark post as posted
        query = "UPDATE reddit_scrapes SET bot_posted = 1 WHERE post_id = '" + unposted_post['post_id'][0] + "'"
        cursor = dbconn.cursor()
        cursor.execute(query)
        dbconn.commit()
        cursor.close()
        dbconn.close()
    except Exception as e:
        traceback.print_exc()
        print("Error:", str(e))
        # mark post as bad
        print("Post created a mastodon error.  Marking as bad")
        query = "UPDATE reddit_scrapes SET bot_posted = 99 WHERE post_id = '" + unposted_post['post_id'][0] + "'"
        cursor = dbconn.cursor()
        cursor.execute(query)
        dbconn.commit()
        cursor.close()
        dbconn.close()
        continue


