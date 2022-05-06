from math import ceil
import tweepy as tw
import pandas as pd
from threading import Thread
from tqdm.auto import tqdm
import os
import json


# DATE = "2022-05-05"

def get_tweet_responses(consumer_key, consumer_secret, access_token, access_token_secret, user, num_tweets, date):

    auth = tw.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tw.API(auth, wait_on_rate_limit=True)

    tweet = tw.Cursor(api.user_timeline,
            user_id=user, count=10,
            exclude_replies=True, include_rts=False,
            tweet_mode='extended').items(num_tweets)

    tweets = []


    tweets.append(dict(
        full_text = tweet.full_text,
        tweet_id=tweet.id_str,
        screen_name=tweet.user.screen_name,
        user_ID=user
    ))
   

    return tweets

def getTweets(token_dict, userid, DATE):
    print(token_dict)
    for user in tqdm(userid):
        tweets = get_tweet_responses(
            token_dict['consumer_key'],
            token_dict['consumer_secret'],
            token_dict['access_token'],
            token_dict['access_token_secret'],
            user, num_tweets=10, date=DATE
        )
        if len(tweets)==0:
            continue
        else:
            pd.DataFrame(tweets).to_csv('User-Tweets/%s.csv' % (user), index=False)

def getTokens(DATE):
    DATE=DATE
    token_arr = []
    with open('tokens') as f:
        tokens = f.read().strip().split('\n')
        for token in tokens:
            consumer_key,consumer_secret,access_token,access_token_secret = token.split('|')
            try:
                get_tweet_responses(consumer_key, consumer_secret, access_token, access_token_secret, 44201535, 1, DATE)
                token_arr.append(dict(consumer_key=consumer_key,consumer_secret=consumer_secret,access_token=access_token,access_token_secret=access_token_secret))
            except Exception as e:
                continue
    return (token_arr)


def run_collection(DATE1):
    DATE = DATE1
    if os.path.exists('working-tokens.json'):
        tokens = json.load(open('working-tokens.json'))
    else:
        tokens = getTokens(DATE)
        json.dump(tokens, open('working-tokens.json', 'w'))

    with open('UserDatabase.csv') as f:
        userid = f.read().strip().split('\n')
        userid=userid.split('.csv')[0]


    NUM_THREADS = len(tokens) #equivalent to num tokens
    userid_PER_THREAD = ceil(len(userid) / len(tokens))

    threads = []
    for i in range(NUM_THREADS):
        token_dict = tokens[i]
        start = int(i * userid_PER_THREAD)
        thread_userid = userid[start: start + userid_PER_THREAD]
        thread = Thread(target=getTweets, args=(token_dict, thread_userid,DATE, ))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()