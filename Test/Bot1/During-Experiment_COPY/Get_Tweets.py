from math import ceil
import tweepy as tw
import pandas as pd
from threading import Thread
from tqdm.auto import tqdm
import os
import json
import sqlite3

os.environ['MKL_THREADING_LAYER'] = 'GNU'

# DATE = "2022-05-05"

def get_tweet_responses(consumer_key, consumer_secret, access_token, access_token_secret, user, num_tweets, since):

    auth = tw.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tw.API(auth, wait_on_rate_limit=True)

    #add sinceid here
    #open database, get sinceid as the max of the previous collected TwitterIDs and add as a parameter
    
    tweets = []
    try:
        tweetsReq = tw.Cursor(api.user_timeline,
            user_id=user, count=10,
            exclude_replies=True, include_rts=False,
            tweet_mode='extended').items(num_tweets)
    
    
        for tweet in tweetsReq:
            tweets.append(dict(
                full_text = tweet.full_text,
                tweet_id=tweet.id_str,
                screen_name=tweet.user.screen_name,
                user_id=tweet.user.id_str
            ))
    except Exception as e:
        print("Key error, moving on..")

    return tweets

def getsinceID(user):
    conn=sqlite3.connect('database.db')
    c=conn.cursor()
    c.execute("SELECT sinceid FROM users WHERE userid=(?)",[user])
    result=c.fetchall()
    print("Got Sinceid")
    conn.commit()
    conn.close()

    return result

def setsinceID(user, sinceid):
    conn=sqlite3.connect('database.db')
    c=conn.cursor()
    c.execute("UPDATE users SET sinceid = (?) WHERE userid = (?)", (sinceid, user))
    print("Set sinceID")
    conn.commit()
    conn.close()



def getTweets(token_dict, userid, GLOBALCOUNT):
    print(token_dict)
    parent_dir=os.path.dirname(os.path.abspath(__file__))
    newdir="User-Tweets/%s" % (GLOBALCOUNT)
    path = os.path.join(parent_dir, newdir)
    #print(path, len(tweets), userid)
    #os.mkdir(path)

    for user in tqdm(userid):

        since1=getsinceID(user)

        tweets = get_tweet_responses(
            token_dict['consumer_key'],
            token_dict['consumer_secret'],
            token_dict['access_token'],
            token_dict['access_token_secret'],
            user, num_tweets=10, since=since1
        )
        if len(tweets)==0: #get_tweet_responses can return None!!
            continue
        else:
            df=pd.DataFrame(tweets)
            Tweetliststr=df['tweet_id'].to_list()
            int_list = list(map(int, Tweetliststr))
            setsinceID(user, max(int_list))
            df.to_csv('User-Tweets/%s/%s.csv' % (GLOBALCOUNT, user), index=False) #WRITE TO PICKLE

def get_tokens():
    token_arr = []
    with open('tokens.txt') as f:
        tokens = f.read().strip().split('\n')
        for token in tokens:
            consumer_key,consumer_secret,access_token,access_token_secret = token.split('|')
            token_arr.append(dict(consumer_key=consumer_key,consumer_secret=consumer_secret,access_token=access_token,access_token_secret=access_token_secret))

    return token_arr


def run_collection(GLOBALCOUNT):
    if os.path.exists('working-tokens.json'):
        tokens = json.load(open('working-tokens.json'))
    else:
        tokens = get_tokens()
        json.dump(tokens, open('working-tokens.json', 'w'))

    with open('UserDatabase1 - Sheet1.csv') as f:
        userid = f.read().strip().split('\n')
        #userid=userid.split('.csv')[0]

    NUM_THREADS = len(tokens) #equivalent to num tokens
    userid_per_thread = ceil(len(userid) / len(tokens))

    threads = []
    for i in range(NUM_THREADS):
        token_dict = tokens[i]
        start = int(i * userid_per_thread)
        thread_userid = userid[start:start + userid_per_thread]
        thread = Thread(target=getTweets, args=(token_dict, thread_userid, GLOBALCOUNT, ))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

   
