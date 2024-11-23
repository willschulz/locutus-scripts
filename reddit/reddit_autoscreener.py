# reddit pre-screening


import pandas as pd
import mysql.connector
from datetime import datetime
import random
import time
import requests
import os

print("Auto-screening session started at: " + str(datetime.now()))


similarity_threshold = .9

title_keywords = ["reddit", "mods", "sub ", "subs ", "downvote", "upvote", "karma", "repost", "r/"]
link_keywords = ["reddit.com", "v.redd.it"]
ocr_keywords = ["reddit", "mods", "sub ", "subs ", "downvote", "upvote", "karma", "repost", "deleted", "image"]

print(os.getcwd())
os.environ['TESSDATA_PREFIX'] = "miniconda3/envs/locutus/share/tessdata"

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


import os
print(os.getcwd())
os.environ['TESSDATA_PREFIX'] = "miniconda3/envs/locutus/share/tessdata"

#for OCR:
import pytesseract
from PIL import Image, ImageOps
from io import BytesIO


# def ocr_images(df):
#     ocr_texts = []
#     for link in df['link']:
#         if link.endswith('.jpg') or link.endswith('.png'):
#             image = Image.open(BytesIO(requests.get(link).content))
#             ocr_text = pytesseract.image_to_string(image)
#             ocr_text2 = pytesseract.image_to_string(ImageOps.autocontrast(ImageOps.invert(image), cutoff=(0,95)))
#             ocr_text = ocr_text + " " + ocr_text2
#             ocr_texts.append(ocr_text)
#         else:
#             ocr_texts.append(None)  # or any default value you prefer
#     df['ocr_text'] = ocr_texts
#     return df


# def ocr_images(posts):
#     processed_posts = []
#     for post in posts:
#         subreddit_id, post_id, created_utc, title, link, permalink = post
#         if link.endswith('.jpg') or link.endswith('.png'):
#             try:
#                 image = Image.open(BytesIO(requests.get(link).content))
#                 if image.mode != 'RGB':
#                     image = image.convert('RGB')
#                 ocr_text = pytesseract.image_to_string(image)
#                 ocr_text2 = pytesseract.image_to_string(ImageOps.autocontrast(ImageOps.invert(image), cutoff=(0,95)))
#                 ocr_text = ocr_text + " " + ocr_text2
#                 print(ocr_text)
#             except Exception as e:
#                 print(f"OCR failed for post {post_id}: {e}")
#                 ocr_text = None  # Enter None if OCR fails
#         else:
#             ocr_text = None  # Enter None for non-image posts
#         processed_posts.append((subreddit_id, post_id, created_utc, title, link, permalink, ocr_text))
#     return processed_posts

def ocr_images(link):
  try:
      image = Image.open(BytesIO(requests.get(link).content))
      if image.mode != 'RGB':
          image = image.convert('RGB')
      ocr_text = pytesseract.image_to_string(image)
      ocr_text2 = pytesseract.image_to_string(ImageOps.autocontrast(ImageOps.invert(image), cutoff=(0,95)))
      ocr_text = ocr_text + " " + ocr_text2
      print(ocr_text)
  except Exception as e:
      print(f"OCR failed for post {row['post_id']}: {e}")
      ocr_text = None  # Enter None if OCR fails
  return ocr_text


#MySQL connection details
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
database = os.getenv("DB_DATABASE")
username = os.getenv("DB_USERNAME")
password = os.getenv("DB_PASSWORD")


#connect to db
dbconn = mysql.connector.connect(
    host=host,
    port=port,
    user=username,
    password=password,
    database=database
)

#check for new posts
unposted_posts = pd.read_sql_query("SELECT * FROM reddit_scrapes WHERE bot_posted = 20 ORDER BY RAND()", dbconn) 

if not unposted_posts.empty:
  for index, row in unposted_posts.iterrows():
    #deduplicate
    # mark as duplicate if there's a duplicate anywhere in the database of un-screened posts other than this one
    old_titles = pd.read_sql_query("SELECT title FROM reddit_scrapes WHERE bot_posted != 20 AND post_id != '" + row['post_id'] + "' ORDER BY created_at_utc DESC LIMIT 1000", dbconn)
    post_too_similar = is_post_too_similar(row['title'], old_titles, similarity_threshold)
    if post_too_similar:
      print(row['title'])
      print("Marking post as duplicate...")
      query = "UPDATE reddit_scrapes SET bot_posted = 98 WHERE post_id = '" + row['post_id'] + "'"
      cursor = dbconn.cursor()
      cursor.execute(query)
      dbconn.commit()
      cursor.close()
      continue
    #censoring
    elif any(keyword in row['title'] for keyword in title_keywords):
      # mark post as censored
      print(row['title'])
      print("Post title contained a banned keyword.  Censoring...")
      query = "UPDATE reddit_scrapes SET bot_posted = 97 WHERE post_id = '" + row['post_id'] + "'"
      cursor = dbconn.cursor()
      cursor.execute(query)
      dbconn.commit()
      cursor.close()
      continue
    elif any(keyword in row['link'] for keyword in link_keywords):
      # mark post as censored
      print(row['link'])
      print("Post link contained a banned keyword.  Censoring...")
      query = "UPDATE reddit_scrapes SET bot_posted = 96 WHERE post_id = '" + row['post_id'] + "'"
      cursor = dbconn.cursor()
      cursor.execute(query)
      dbconn.commit()
      cursor.close()
      continue
    ##ocr
    else:
      if row['link'].endswith('.jpg') or row['link'].endswith('.png'):
        print(row['link'])
        row['ocr_text'] = ocr_images(row['link'])
        query = """UPDATE reddit_scrapes SET ocr_text = %s WHERE post_id = %s"""
        #query = "UPDATE reddit_scrapes SET ocr_text = '" + row['ocr_text'] + "' WHERE post_id = '" + row['post_id'] + "'"
        cursor = dbconn.cursor()
        cursor.execute(query, [row['ocr_text'], row['post_id']])
        dbconn.commit()
        cursor.close()
        if (row['ocr_text'] and any(keyword in row['ocr_text'] for keyword in ocr_keywords)):
          # mark post as censored
          print("Post ocr contained a banned keyword.  Censoring...")
          query = "UPDATE reddit_scrapes SET bot_posted = 95 WHERE post_id = '" + row['post_id'] + "'"
          cursor = dbconn.cursor()
          cursor.execute(query)
          dbconn.commit()
          cursor.close()
          continue
      print(row['title'])
      print("Post looks good!")
      # add flavor prescreening here
      this_flavor = pd.read_sql_query("SELECT flavor FROM sources WHERE name = '" + row['subreddit_name'] + "'", dbconn)['flavor'][0] #adjust to subset to bots with active scripts?
      if this_flavor == "political_l":
        if row['link'].endswith('.jpg') or row['link'].endswith('.png'):
          query = "UPDATE reddit_scrapes SET bot_posted = 23 WHERE post_id = '" + row['post_id'] + "'"
        else: 
          query = "UPDATE reddit_scrapes SET bot_posted = 24 WHERE post_id = '" + row['post_id'] + "'"
      elif this_flavor == "apolitical":
        query = "UPDATE reddit_scrapes SET bot_posted = 22 WHERE post_id = '" + row['post_id'] + "'"
      else:
        query = "UPDATE reddit_scrapes SET bot_posted = 21 WHERE post_id = '" + row['post_id'] + "'"
      cursor = dbconn.cursor()
      cursor.execute(query)
      dbconn.commit()
      cursor.close()


dbconn.close()


