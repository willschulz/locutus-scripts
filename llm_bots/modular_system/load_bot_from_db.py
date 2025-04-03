## OLD

# demo_bot_id = 113314610954653850

# from mastodon import Mastodon
# import pandas as pd
# #from datetime import datetime
# import mysql.connector
# #import time
# import os
# import json

# # MySQL connection details
# host = os.getenv("DB_HOST")
# port = os.getenv("DB_PORT")
# database = os.getenv("DB_DATABASE")
# username = os.getenv("DB_USERNAME")
# password = os.getenv("DB_PASSWORD")

# dbconn = mysql.connector.connect(
#     host=host,
#     port=port,
#     user=username,
#     password=password,
#     database=database
# )

# # Initiate scraping session with token from random user -- probably should use my admin account when I figure out how to remove all rate limits
# user_data = pd.read_sql_query("SELECT * FROM mirror_accounts WHERE id = " + str(demo_bot_id), dbconn)
# print(user_data)

# # # Collect Memories from Database # memories are not currently being created, so this isn't being sued
# # memories = pd.read_sql_query("SELECT * FROM memories WHERE user_id = '" + str(user_id) + "'", dbconn)
# # # Filter memories by importance and recency
# # filtered_memories = memories[(memories['importance'] > 2) | (memories['timestamp'] > pd.Timestamp.now() - pd.Timedelta(days=1))]

# favoritisms = pd.read_sql_query("SELECT * FROM favoritisms WHERE user_id = '" + str(user_data["id"].values[0]) + "'", dbconn)



# ### End of database-focused user-info-loading module?

# #user_token = user_data["token"].values[0]
# #user_id = user_data["id"].values[0]
# #persona = user_data["persona"].values[0] #not yet used, but already has a column in the database


# import openai

# openai.api_key = os.getenv("OPENAI_KEY")

# # Define a function to get a response from the OpenAI API
# def get_openai_response(system_instruction, prompt):
#     try:
#         # Call the OpenAI API using the new interface
#         response = openai.chat.completions.create(
#             model="gpt-4o-mini",  # Use the appropriate model
#             messages=[
#                 {"role": "system", "content": system_instruction},
#                 {"role": "user", "content": prompt}
#             ],
#             max_tokens=400  # Adjust the number of tokens as needed
#         )
#         # Extract the text from the response
#         return response
#     except Exception as e:
#         return f"An error occurred: {e}"

# #Make Mastodon Connection
# mastodon = Mastodon(access_token = user_data["token"].values[0], api_base_url = 'https://beta.argyle.social') #remember to generalize this to other instances

# # Iterate through timeline to simulate a user browsing, using the persona and experiences to determine whether the bot engages with one of the posts in the current browsing window, or generates a new post, or else "scrolls" on to the next window of posts

# bored = 0
# t = None
# while bored < 1:
#     # Fetch the most recent or next batch of posts
#     if t is None:
#         t = mastodon.timeline('local', limit=10)
#     else:
#         t = mastodon.timeline('local', limit=10, max_id=t[-1]['id'])
    
#     # prepare the posts to be shown to GPT for evaluation, by pasting together the post content and identifying it with the id, and turning it into one well-formatted text string
#     print("Now Evaluating: ")
#     posts_to_evaluate = ""
#     for post in t:
#         #print post with id on each iteration
#         #print(post['id'])
#         #print(post['content'])
#         posts_to_evaluate += f"Post ID: {post['id']}\nPost Content: {post['content']}\n\n"

#     # Create persona and memories text
#     persona_text = f"Persona:\n{persona}\n\n"
#     memories_text = "Memories:\n" + "\n".join(filtered_memories['memory'].tolist()) + "\n\n"

#     # Create the system instruction
#     timeline_perception_prompt = (
#         "You are a social media user with the following persona:\n" + persona_text +
#         "\nYou also have the following recent and important memories:\n" + memories_text +
#         "\nYou are browsing your social media timeline and you see the following posts in JSON format. "
#         "Decide what action to take for each post. You can choose to 'like', 'reply', 'boost', or 'new_post'. "
#         "Reply with a JSON array where each element specifies your action and the post ID and text content of the action if applicable. "
#         "For example:\n[\n  {\"action\": \"like\", \"post_id\": \"1234567890\"},\n {\"action\": \"reply\", \"post_id\": \"1234567890\", \"content\": \"...\"}\n]\n If nothing in the current set of posts interests you, respond with only [{\"action\": \"scroll\"}] and no other actions.  Return only JSON, do not return any markdown."
#     )

#     ## Convert posts to JSON string
#     #posts_to_evaluate_json = json.dumps(posts_to_evaluate, indent=2)

#     # Call the OpenAI API
#     #full_prompt = timeline_perception_prompt + "\n\nPosts:\n" + posts_to_evaluate
#     response = get_openai_response(timeline_perception_prompt, posts_to_evaluate)
#     print(response)
#     response = response.choices[0].message.content

#     # Parse the JSON response and take actions on Mastodon
#     try:
#         actions = json.loads(response)
#         for action in actions:
#             print(action)
#             if action["action"] == "like":
#                 mastodon.status_favourite(action["post_id"])
#                 print(f"Liked post ID: {action['post_id']}")
#             elif action["action"] == "reply":
#                 reply_content = action.get("content", "No reply content provided")
#                 mastodon.status_post(status=reply_content, in_reply_to_id=action["post_id"])
#                 print(f"Replied to post ID: {action['post_id']} with content: {reply_content}")
#             elif action["action"] == "boost":
#                 mastodon.status_reblog(action["post_id"])
#                 print(f"Boosted post ID: {action['post_id']}")
#             elif action["action"] == "new_post":
#                 new_post_content = action.get("content", "No new post content provided")
#                 mastodon.status_post(status=new_post_content)
#                 print(f"Created new post with content: {new_post_content}")
#             elif action["action"] == "scroll":
#                 print(f"Scrolling on...")
#             else:
#                 print(f"Unknown action: {action['action']}")
#     except json.JSONDecodeError:
#         print("Error parsing JSON response:")
#         print(response)

#     print("Finished evaluating batch.\n\n")
#     #print(f"Scrolling on...")
#     bored += 0.34

# dbconn.close()