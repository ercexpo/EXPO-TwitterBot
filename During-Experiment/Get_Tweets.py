from math import ceil
import tweepy as tw
import pandas as pd
from threading import Thread
from tqdm.auto import tqdm
import os
import json
import sqlite3


# DATE = "2022-05-05"

def get_tweet_responses(consumer_key, consumer_secret, access_token, access_token_secret, user, num_tweets, since):

    auth = tw.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tw.API(auth, wait_on_rate_limit=True)

    #add sinceid here
    #open database, get sinceid as the max of the previous collected TwitterIDs and add as a parameter
    

    tweet = tw.Cursor(api.user_timeline,
            user_id=user, count=10,
            since_id=since,
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

def getsinceID(user):
    conn=sqlite3.connect('database.db')
    c=conn.cursor()
    c.execute("SELECT sinceid FROM users WHERE userid=(?)",user)
    result=c.fetch()
    print("Got Sinceid")
    conn.commit()
    conn.close()

    return result

def setsinceID(user, sinceid):
    conn=sqlite3.connect('database.db')
    c=conn.cursor()
    c.execute("UPDATE users SET sinceid = (?) WHERE userid = (?)", sinceid, user)
    print("Set sinceID")
    conn.commit()
    conn.close()



def getTweets(token_dict, userid, DATE):
    print(token_dict)
    for user in tqdm(userid):

        since1=getsinceID(user)

        tweets = get_tweet_responses(
            token_dict['consumer_key'],
            token_dict['consumer_secret'],
            token_dict['access_token'],
            token_dict['access_token_secret'],
            user, num_tweets=10, date=DATE, since=since1
        )
        if len(tweets)==0:
            continue
        else:
            df=pd.DataFrame(tweets)
            Tweetliststr=df['tweet_id'].to_list
            int_list = list(map(int, Tweetliststr))
            setsinceID(user, max(int_list))
            df.to_csv('User-Tweets/%s.csv' % (user), index=False)

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