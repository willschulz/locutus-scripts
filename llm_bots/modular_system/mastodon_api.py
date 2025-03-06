# mastodon_api.py
from mastodon import Mastodon

def create_mastodon_client(access_token, api_base_url):
    return Mastodon(access_token=access_token, api_base_url=api_base_url)

def perform_action(mastodon, action):
    try:
        if action["action"] == "like":
            mastodon.status_favourite(action["post_id"])
        elif action["action"] == "reply":
            mastodon.status_post(status=action["content"], in_reply_to_id=action["post_id"])
        elif action["action"] == "boost":
            mastodon.status_reblog(action["post_id"])
        elif action["action"] == "new_post":
            mastodon.status_post(status=action["content"])
    except Exception as e:
        print(f"Error performing action {action}: {e}")