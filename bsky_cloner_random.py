
# def make_clean_name(name):
#     #remove anything following a . or @, including the . or @
#     #name = name.split('.')[0]
#     #name = name.split('@')[0]
#     #remove ".bsky.social" if it's there
#     name = name.replace('.bsky.social', '')
#     name = name.replace('.com', '')
#     #replace any remaining . with _
#     name = name.replace('.', '_')
#     return name

# import py_functions.account_creation

# from mastodon import Mastodon
# import pandas as pd
# import mysql.connector
# from datetime import datetime
# import os

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

# # want: function to which I supply instance and content, so can be fired off independently

# instance_base_url = 'https://alpha.argyle.social' #to do: make it possible to clone the same bots on multiple servers
# #instance_base_urls = ['https://alpha.argyle.social', 'https://beta.argyle.social']

# unposted_post = pd.read_sql_query("SELECT * FROM bsky_posts ORDER BY RAND() LIMIT 1", dbconn).iloc[0] #todo: make it not be random, but instead determined by server-level settings
# #unposted_post = pd.read_sql_query("SELECT * FROM bsky_posts ORDER BY post_created_at DESC LIMIT 1", dbconn)
# #todo: prevent repeat-posting (check this at the server level)

# #turn this into a function that can be called independently, providing the instance_base_url and the post and the dbconn:
# post_did = unposted_post['author_did']
# #check the mirror_accounts table and return a logical for whether or not there is already an account with this instance_base_url and clone_user_id equal to post_did
# account_exists = pd.read_sql_query("SELECT * FROM mirror_accounts WHERE instance_base_url = '" +
#                                 instance_base_url +
#                                 "' AND clone_user_id = '" +
#                                 post_did +
#                                 "'",
#                                 dbconn).shape[0] > 0

# #if the account doesn't exist
# if not account_exists:
#     #create a new account
#     #split instance_base_url into subdomain and domain
#     subdomain = instance_base_url.split('.')[0]
#     #removing 'https://' from the beginning
#     subdomain = subdomain.split('//')[1]
#     domain = '.'.join(instance_base_url.split('.')[1:])
#     current_user = py_functions.account_creation.create_account(name = make_clean_name(unposted_post['author_handle']),
#                                                 subdomain = subdomain,
#                                                 domain = domain,
#                                                 type = 'bsky_clone',
#                                                 clone_user_id = post_did,
#                                                 all_follow = True,
#                                                 avatar_image=unposted_post['author_avatar']).iloc[0]
# else:
#     #select the account to post from
#     current_user = pd.read_sql_query(f"SELECT * FROM mirror_accounts WHERE clone_user_id = '" + post_did + "'", dbconn).iloc[0]

# # then post the content via the bot
# print(unposted_post['post_text'])
# print("Attempting to post " + unposted_post['post_cid'] + " at " + str(datetime.now()))

# mastodon = Mastodon(access_token=current_user['token'], api_base_url=current_user['instance_base_url'])

# if unposted_post['embed_external_uri']=='' and unposted_post['embed_image_uri']=='':
#     posted_status = mastodon.status_post(unposted_post['post_text'])
# # logical to check whether there is an embed_external_uri
# if unposted_post['embed_external_uri']!='' and unposted_post['embed_image_uri']=='':
#     posted_status = mastodon.status_post(unposted_post['post_text'] + "\n" + unposted_post['embed_external_uri'])

# # if link.endswith('.jpg') or link.endswith('.png'):
# #     media_type = "image/jpeg" if link.endswith('.jpg') else "image/png"
# #     media = mastodon.media_post(requests.get(link).content, media_type)
# #     posted_status = mastodon.status_post(title, media_ids=media)
# # else:
# #     title = title + "\n" + link #make a version that allows for posting without a link?
# #     posted_status = mastodon.status_post(title)
# # mark post as posted


# def post_to_instance(instance_base_url, unposted_post, dbconn):
#     import pandas as pd
#     from mastodon import Mastodon
#     from datetime import datetime
#     import py_functions.account_creation

#     def make_clean_name(name):
#         # Remove ".bsky.social" and ".com" if present
#         name = name.replace('.bsky.social', '')
#         name = name.replace('.com', '')
#         # Replace any remaining '.' with '_'
#         name = name.replace('.', '_')
#         return name

#     post_did = unposted_post['author_did']

#     # Check if the account already exists in mirror_accounts
#     query = """
#     SELECT * FROM mirror_accounts 
#     WHERE instance_base_url = %s AND clone_user_id = %s
#     """
#     account_exists = pd.read_sql_query(query, dbconn, params=(instance_base_url, post_did)).shape[0] > 0

#     if not account_exists:
#         # Create a new account
#         # Remove 'https://' or 'http://' from the beginning
#         url_without_protocol = instance_base_url.replace('https://', '').replace('http://', '')
#         url_parts = url_without_protocol.split('.')
#         subdomain = url_parts[0]
#         domain = '.'.join(url_parts[1:])

#         current_user = py_functions.account_creation.create_account(
#             name=make_clean_name(unposted_post['author_handle']),
#             subdomain=subdomain,
#             domain=domain,
#             type='bsky_clone',
#             clone_user_id=post_did,
#             all_follow=True,
#             avatar_image=unposted_post['author_avatar']
#         ).iloc[0]
#     else:
#         # Select the existing account
#         current_user = pd.read_sql_query(
#             "SELECT * FROM mirror_accounts WHERE clone_user_id = %s",
#             dbconn, params=(post_did,)
#         ).iloc[0]

#     # Post the content via the bot
#     print(unposted_post['post_text'])
#     print(f"Attempting to post {unposted_post['post_cid']} at {datetime.now()}")

#     mastodon = Mastodon(
#         access_token=current_user['token'],
#         api_base_url=current_user['instance_base_url']
#     )

#     if unposted_post['embed_external_uri'] == '' and unposted_post['embed_image_uri'] == '':
#         posted_status = mastodon.status_post(unposted_post['post_text'])
#     elif unposted_post['embed_external_uri'] != '' and unposted_post['embed_image_uri'] == '':
#         posted_status = mastodon.status_post(
#             f"{unposted_post['post_text']}\n{unposted_post['embed_external_uri']}"
#         )
#     # Handle other cases if necessary

#     # Mark the post as posted in the database
#     cursor = dbconn.cursor()
#     #update_query = "UPDATE bsky_posts SET posted = 1 WHERE post_cid = %s" #todo: make this work
#     cursor.execute(update_query, (unposted_post['post_cid'],))
#     dbconn.commit()
#     cursor.close()


# # to do later:
# # create table for posted content and check against it when selecting new content


def make_clean_name(name):
        # Remove ".bsky.social" and ".com" if present
        name = name.replace('.bsky.social', '')
        name = name.replace('.com', '')
        # Replace any remaining '.' with '_'
        name = name.replace('.', '_')
        return name


def post_to_instance(instance_base_url, unposted_post, dbconn):
    import pandas as pd
    from mastodon import Mastodon
    from datetime import datetime
    import py_functions.account_creation

    post_did = unposted_post['author_did']

    # Check if the account already exists in mirror_accounts
    query = """
    SELECT * FROM mirror_accounts 
    WHERE instance_base_url = %s AND clone_user_id = %s
    """
    account_exists = pd.read_sql_query(query, dbconn, params=(instance_base_url, post_did)).shape[0] > 0

    if not account_exists:
        # Create a new account
        # Remove 'https://' or 'http://' from the beginning
        url_without_protocol = instance_base_url.replace('https://', '').replace('http://', '')
        url_parts = url_without_protocol.split('.')
        subdomain = url_parts[0]
        domain = '.'.join(url_parts[1:])

        current_user = py_functions.account_creation.create_account(
            name=make_clean_name(unposted_post['author_handle']),
            subdomain=subdomain,
            domain=domain,
            type='bsky_clone',
            clone_user_id=post_did,
            all_follow=True,
            avatar_image=unposted_post['author_avatar']
        ).iloc[0]
    else:
        # Select the existing account
        query = """
        SELECT * FROM mirror_accounts 
        WHERE instance_base_url = %s AND clone_user_id = %s
        """
        current_user = pd.read_sql_query(query, dbconn, params=(instance_base_url, post_did)).iloc[0]

    # Post the content via the bot
    print(unposted_post['post_text'])
    print(f"Attempting to post {unposted_post['post_cid']} at {datetime.now()}")

    mastodon = Mastodon(
        access_token=current_user['token'],
        api_base_url=current_user['instance_base_url']
    )

    if unposted_post['embed_external_uri'] == '' and unposted_post['embed_image_uri'] == '':
        posted_status = mastodon.status_post(unposted_post['post_text'])
    elif unposted_post['embed_external_uri'] != '' and unposted_post['embed_image_uri'] == '':
        posted_status = mastodon.status_post(
            f"{unposted_post['post_text']}\n{unposted_post['embed_external_uri']}"
        )
    # Handle other cases if necessary

    # Mark the post as posted in the database
    #cursor = dbconn.cursor()
    #update_query = "UPDATE bsky_posts SET posted = 1 WHERE post_cid = %s"
    #cursor.execute(update_query, (unposted_post['post_cid'],))
    #dbconn.commit()
    #cursor.close()


def post_to_instances(instance_base_urls, unposted_post, db_config):
    import threading
    import mysql.connector

    def thread_function(instance_base_url):
        # Each thread creates its own database connection
        dbconn = mysql.connector.connect(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database']
        )
        try:
            post_to_instance(instance_base_url, unposted_post, dbconn)
        finally:
            dbconn.close()

    threads = []
    for instance_base_url in instance_base_urls:
        thread = threading.Thread(target=thread_function, args=(instance_base_url,))
        threads.append(thread)
        thread.start()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

# # Demo Usage:

# import os
# host = os.getenv("DB_HOST")
# port = os.getenv("DB_PORT")
# database = os.getenv("DB_DATABASE")
# username = os.getenv("DB_USERNAME")
# password = os.getenv("DB_PASSWORD")

# # Database configuration
# db_config = {
#     'host': host,
#     'port': port,
#     'user': username,
#     'password': password,
#     'database': database
# }



# Diurnal Probability Implementation:

def random_bsky_post_cloner(instance_base_urls = ['https://alpha.argyle.social', 'https://beta.argyle.social'], db_config = None):
    import pandas as pd
    import mysql.connector

    # Fetch an unposted post (example)
    dbconn = mysql.connector.connect(
        host=db_config['host'],
        port=db_config['port'],
        user=db_config['user'],
        password=db_config['password'],
        database=db_config['database']
    )

    # unposted_post = pd.read_sql_query(
    #     "SELECT * FROM bsky_posts WHERE posted = 0 ORDER BY RAND() LIMIT 1",
    #     dbconn
    # ).iloc[0]
    unposted_post = pd.read_sql_query("SELECT * FROM bsky_posts ORDER BY RAND() LIMIT 1", dbconn).iloc[0] #todo: make it not be random, but instead determined by server-level settings

    dbconn.close()

    # Call the function
    post_to_instances(instance_base_urls, unposted_post, db_config)

import os
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
database = os.getenv("DB_DATABASE")
username = os.getenv("DB_USERNAME")
password = os.getenv("DB_PASSWORD")

# Database configuration
db_config = {
    'host': host,
    'port': port,
    'user': username,
    'password': password,
    'database': database
}

import py_functions.diurnal_patterns

#py_functions.diurnal_patterns.execute_with_diurnal_prob(random_bsky_post_cloner, args=(['https://alpha.argyle.social', 'https://beta.argyle.social'], db_config), event_weight=.01, duration=59)

# to do:
## make recency, nonduplication, and parallel logging work
## prevent case where account creation starts twice for the same clone-ee
## Solution: add a status code to the bsky_posts table that indicates "in progress" or "posted" or "failed" or "duplicate"


# Demo Usage:
instance_base_urls = ['https://alpha.argyle.social', 'https://beta.argyle.social']

import pandas as pd
import mysql.connector

# Fetch an unposted post (example)
dbconn = mysql.connector.connect(
    host=host,
    port=port,
    user=username,
    password=password,
    database=database
)

# unposted_post = pd.read_sql_query(
#     "SELECT * FROM bsky_posts WHERE posted = 0 ORDER BY RAND() LIMIT 1",
#     dbconn
# ).iloc[0]
unposted_post = pd.read_sql_query("SELECT * FROM bsky_posts ORDER BY RAND() LIMIT 1", dbconn).iloc[0] #todo: make it not be random, but instead determined by server-level settings

dbconn.close()

# Call the function
post_to_instances(instance_base_urls, unposted_post, db_config)