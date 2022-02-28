import tweepy
import pandas as pd

CLIENT_KEY = ''
SECRET_KEY = ''
BEARER_TOKEN = ''

# Elon Musks' Twitter Id
userid = 44196397

twitter = tweepy.Client(BEARER_TOKEN, CLIENT_KEY, SECRET_KEY)

tweets = []
for tweet in tweepy.Paginator(twitter.get_users_tweets,
                              userid,
                              max_results=100,
                              exclude='retweets,replies',
                              tweet_fields=['id', 'text',
                                            'created_at']).flatten(3200):
    tweets.append(tweet)

df = pd.DataFrame(tweets)
df.to_csv('/akbar/home/tweets.csv', mode='w', index=False)
