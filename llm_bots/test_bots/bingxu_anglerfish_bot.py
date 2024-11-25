# FOR WILL

bot_name = 'anglerfish'

from mastodon import Mastodon
import pandas as pd
import mysql.connector
import os

import openai

openai.api_key = os.getenv("OPENAI_KEY")

# Define a function to get a response from the OpenAI API
def get_openai_response(system_instruction, prompt):
    try:
        # Call the OpenAI API using the new interface
        response = openai.chat.completions.create(
            model="gpt-4o-mini",  # Use the appropriate model
            messages=[
                {"role": "system", "content": system_instruction},
                {"role": "user", "content": prompt}
            ],
            max_tokens=150  # Adjust the number of tokens as needed
        )
        # Extract the text from the response
        return response
    except Exception as e:
        return f"An error occurred: {e}"

# MySQL connection details
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

# Initiate scraping session with token from random user -- probably should use my admin account when I figure out how to remove all rate limits
user_token = pd.read_sql_query("SELECT token FROM mirror_accounts WHERE aid = '" + bot_name + ".beta.argyle.social'", dbconn)["token"].values[0]
mastodon = Mastodon(access_token = user_token, api_base_url = 'https://beta.argyle.social')
t = mastodon.timeline('local', limit = 10)

t.sort(key=lambda x: x['created_at'], reverse=True)
most_recent_post = t[0] if t else None

post_id_to_reply_to = most_recent_post['id']
account_acct_to_reply_to = most_recent_post['account']['acct']
content_to_reply_to = most_recent_post['content']

system_instruction_supportive = "You are a typical Twitter user. \
    You are given a tweet and you need to generate a response to the tweet. \
        The reply must have a internet-friendly spin with abbreviations and cyber lingo.\
        Do not start with 'Reply:'. \
        Just write up the tweet response.\
        Make sure the response is supportive of the original poster's original point, and adds a thematically related point."

text_of_reply = get_openai_response(system_instruction_supportive, content_to_reply_to).choices[0].message.content

posted_status = mastodon.status_post("@" + str(account_acct_to_reply_to) + " " + text_of_reply, in_reply_to_id = post_id_to_reply_to)

dbconn.close()
