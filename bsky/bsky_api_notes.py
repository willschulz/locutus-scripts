# Notes on bksy api endpoints / "HTTP Reference"
from atproto import Client
import os
client = Client(base_url='https://bsky.social')
client.login(os.getenv("BSKY_EMAIL"), os.getenv("BSKY_PASSWORD"))


# Get get user info
#app.bsky.actor.getProfile

# "Get like records which reference a subject (by AT-URI and CID)." (test: can the subject AT-URI be a post? a user?  Where do I get a CID?)
#app.bsky.feed.getLikes

this_uri = ''
this_cid = ''


print(f"Scraping : {row['feed_name']}")
data = client.app.bsky.feed.get_likes({
    'feed': row['feed_at'],
    #'feed': 'at://did:plc:z72i7hdynmk6r22z27h6tvur/app.bsky.feed.generator/whats-hot',
    'limit': 30,
}, headers={'Accept-Language': 'en'})

print(data)

# Gets posts and reposts from a specific author (no auth needed) (do we get like counts for these posts? could be useful)
#app.bsky.feed.getAuthorFeed

# "Enumerates accounts which a specified account (actor) follows." (useful for constructing follow-based timeline for a panel; use in conjunction with getAuthorFeed)
#app.bsky.graph.getFollows

# "Enumerates accounts which follow a specified account (actor)." (good to know about)
#app.bsky.graph.getFollowers

# Gets recent posts from a list of actors (Could be useful if I want to use lists to organize scraping panels)
#app.bsky.feed.getListFeed

# Gets OWN account likes (not useful for training predictive engagement model)
#app.bsky.feed.getActorLikes

# Gets OWN timeline (not useful for panel tracking / training predictive engagement model)
#app.bsky.feed.getTimeline

# Gets "hydrated feed from an actor's selected feed generator" (what I'm already using for the bsky_feeds_scraper.py)
#app.bsky.feed.getFeed

### Relationship Stuff

# "Enumerates public relationships between one account, and a list of other accounts. Does not require auth." (useful only if we know a reference set)
#app.bsky.graph.getRelationships


### Starter Pack Stuff

# Get starter packs CREATED BY a specific actor (not useful)
#app.bsky.graph.getActorStarterPacks

# "Get views for a list of starter packs/a starter pack." (what is this?)
#app.bsky.graph.getStarterPacks
#app.bsky.graph.getStarterPack



# Get suggested accounts to follow
#app.bsky.actor.getSuggestions

# Gets a list of the feeds the actor has created (not the ones they actually follow? -- test)
#app.bsky.feed.getActorFeeds

# Gets posts in a thread, useful if I want to study reply threads
#app.bsky.feed.getPostThread

# Hydrates posts based on a set of supplied URIs
#app.bsky.feed.getPosts