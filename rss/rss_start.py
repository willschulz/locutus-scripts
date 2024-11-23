# #simple version
# import feedparser
# 
# nyt_rss_feed = 'https://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml'
# 
# feed = feedparser.parse(nyt_rss_feed)
# 
# for entry in feed.entries:
#     print('Title:', entry.title)
#     print('Link:', entry.link)
#     print('Summary:', entry.summary)
#     print('Published:', entry.published)
#     print('-------------------------------')

# import feedparser
# from datetime import datetime, timezone
# 
# minutes = 60
# 
# #url = 'https://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml'
# url = 'https://moxie.foxnews.com/google-publisher/politics.xml' #fox politics
# 
# feed = feedparser.parse(url)
# 
# now = datetime.now(timezone.utc)
# 
# for entry in feed.entries:
#     published_time = datetime.strptime(entry.published, '%a, %d %b %Y %H:%M:%S %z')
#     if (now - published_time).total_seconds() <= 60*minutes:  # minutes to seconds
#         print('Title:', entry.title)
#         print('Link:', entry.link)
#         print('Published:', entry.published)
#         print('Summary:', entry.summary)
#         print('-'*50)

# import feedparser
# import datetime
# import pytz
# 
# #url = 'https://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml'
# url = 'https://moxie.foxnews.com/google-publisher/politics.xml' #fox politics
# 
# feed = feedparser.parse(url)
# 
# for entry in feed.entries:
#     published_time = datetime.datetime.strptime(entry.published, '%a, %d %b %Y %H:%M:%S %z')
#     published_timezone = pytz.timezone(published_time.tzname())
#     published_time = published_time.astimezone(published_timezone)
# 
#     now = datetime.datetime.now(pytz.utc).astimezone(published_timezone)
# 
#     if (now - published_time).total_seconds() <= 1200:  # 20 minutes in seconds
#         print('Title:', entry.title)
#         print('Link:', entry.link)
#         print('Published:', entry.published)
#         print('Summary:', entry.summary)
#         print('-'*50)


# import feedparser
# import datetime
# import pytz
# import re
# 
# minutes = 600
# 
# #url = 'https://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml'
# url = 'https://moxie.foxnews.com/google-publisher/politics.xml' #fox politics
# 
# feed = feedparser.parse(url)
# 
# entry = feed.entries[0]
# published_time_str = entry.published
# 
# print(published_time_str)
# 
# try:
#     published_time = datetime.datetime.strptime(published_time_str, '%a, %d %b %Y %H:%M:%S %Z')
# except ValueError:
#     published_time = datetime.datetime.strptime(published_time_str, '%a, %d %b %Y %H:%M:%S %z')
# 
# print(published_time)
# print(published_time.tzname())
# 
# #now = datetime.datetime.now()
# 
# for entry in feed.entries:
#     published_time_str = entry.published
#     
#     # Try to parse using common date formats
#     try:
#         published_time = datetime.datetime.strptime(published_time_str, '%a, %d %b %Y %H:%M:%S %Z')
#     except ValueError:
#         try:
#             published_time = datetime.datetime.strptime(published_time_str, '%a, %d %b %Y %H:%M:%S %z')
#         except ValueError:
#             continue
#             # try:
#             #     published_time = datetime.datetime.strptime(published_time_str, '%Y-%m-%dT%H:%M:%S%z')
#             # except ValueError:
#             #     # Use regular expressions to extract date and timezone
#             #     match = re.search(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}).*?([+-]\d{2}:?\d{2})', published_time_str)
#             #     if match:
#             #         date_str, tz_str = match.groups()
#             #         published_time = datetime.datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S')
#             #         published_time = pytz.timezone(tz_str).localize(published_time)
#             #     else:
#             #         continue  # Skip entry if unable to parse published time
#     print(published_time)
#     
#     print(published_time.tzname())
#     
#     published_timezone = pytz.timezone(published_time.tzname())
#     published_time = published_time.astimezone(published_timezone)
#     now = datetime.datetime.now(pytz.utc).astimezone(published_timezone)
#     
#     #now = datetime.datetime.now(pytz.utc).astimezone(published_timezone)
#     
#     if (now - published_time).total_seconds() <= 60*minutes:  # 20 minutes in seconds
#         print('Title:', entry.title)
#         print('Link:', entry.link)
#         print('Published:', entry.published)
#         print('Summary:', entry.summary)
#         print('-'*50)


import feedparser
import datetime
import pytz
#import re
import dateutil.parser

minutes = 60

url = 'https://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml'
#url = 'https://moxie.foxnews.com/google-publisher/politics.xml' #fox politics

feed = feedparser.parse(url)

# entry = feed.entries[0]
# published_time_str = entry.published
# print(published_time_str)
# print(published_time)
# print(published_time.tzname())

#now = datetime.datetime.now()

for entry in feed.entries:
    published_time_str = entry.published
    published_time = dateutil.parser.parse(published_time_str)
    #print(published_time)
    #print(published_time.tzname())
    
    published_timezone = pytz.timezone(published_time.tzname())
    published_time = published_time.astimezone(published_timezone)
    now = datetime.datetime.now(pytz.utc).astimezone(published_timezone)
    
    #now = datetime.datetime.now(pytz.utc).astimezone(published_timezone)
    
    if (now - published_time).total_seconds() <= 60*minutes:  # 20 minutes in seconds
        print('Title:', entry.title)
        print('Link:', entry.link)
        print('Published:', entry.published)
        print('Summary:', entry.summary)
        print('-'*50)
