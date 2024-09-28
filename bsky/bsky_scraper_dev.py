

#need: pip install atproto

from atproto import Client

#print information about Client class
print(Client.__doc__)

client = Client(base_url='https://bsky.social')
client.login('wm.s.schulz@gmail.com', 'MKe8rL7ZS5b4hkS') #to do: move credentials into environment variables

data = client.app.bsky.feed.get_feed({
    'feed': 'at://did:plc:z72i7hdynmk6r22z27h6tvur/app.bsky.feed.generator/whats-hot',
    'limit': 30,
}, headers={
    #'Accept-Language': preferred_languages
})

feed = data.feed
next_page = data.cursor

print(feed)

#print information about feed
print(feed.__doc__)

#identify the structure of the feed object
print(feed[0])

#parse feed into a pandas dataframe
import pandas as pd
from bsky_functions import parse_feedviewpost_list

df = parse_feedviewpost_list(feed)
print(df)
