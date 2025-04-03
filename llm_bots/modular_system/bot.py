# bot.py
import os
import sys
from database import get_bot_data, get_favoritisms
from mastodon_api import create_mastodon_client, perform_action
from ai_decision import get_openai_response
import json

def fetch_timeline(mastodon, max_id=None, limit=10):
    return mastodon.timeline('local', limit=limit, max_id=max_id)

def process_timeline(mastodon, persona, timeline):
    posts_to_evaluate = "\n".join([f"Post ID: {post['id']}\nPost Content: {post['content']}" for post in timeline])
    #system_instruction = f"You are a social media user with the persona: {persona}. Analyze posts and decide actions."
    system_instruction = (
        "You are a social media user with the following persona:\n" + persona +
       #"\nYou also have the following recent and important memories:\n" + memories_text +
        "\nYou are browsing your social media timeline and you see the following posts in JSON format. "
        "Decide what action to take for each post. You can choose to 'like', 'reply', 'boost', or 'new_post'. "
        "Reply with a JSON array where each element specifies your action and the post ID and text content of the action if applicable. "
        "For example:\n[\n  {\"action\": \"like\", \"post_id\": \"1234567890\"},\n {\"action\": \"reply\", \"post_id\": \"1234567890\", \"content\": \"...\"}\n]\n If nothing in the current set of posts interests you, respond with only [{\"action\": \"scroll\"}] and no other actions.  Return only JSON, do not return any markdown."
    )
    actions = get_openai_response(system_instruction, posts_to_evaluate)
    actions = json.loads(actions)
    for action in actions:
        perform_action(mastodon, action)

def main(bot_id):
    bot_data = get_bot_data(bot_id)
    if bot_data.empty:
        print("Bot data not found.")
        return
    
    #favoritisms = get_favoritisms(bot_id) #to do: use this in the AI decision

    access_token = bot_data["token"].values[0]
    api_base_url = "https://beta.argyle.social"
    mastodon = create_mastodon_client(access_token, api_base_url)
    persona = bot_data["persona"].values[0]
    
    bored = 0
    t = None
    while bored < 1:
        t = fetch_timeline(mastodon, max_id=t[-1]['id'] if t else None)
        process_timeline(mastodon, persona, t)
        bored += 0.34

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python bot.py <bot_id>") #"113314610954653850"
        sys.exit(1)
    main(int(sys.argv[1]))