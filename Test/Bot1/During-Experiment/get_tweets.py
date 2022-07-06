from math import ceil
import tweepy as tw
import pandas as pd
from threading import Thread
from tqdm.auto import tqdm
import os
import json
import sqlite3
from collections import defaultdict
import json

os.environ['MKL_THREADING_LAYER'] = 'GNU'

def get_tweet_responses(consumer_key, consumer_secret, access_token, access_token_secret, user, num_tweets, since):

    auth = tw.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tw.API(auth, wait_on_rate_limit=True)
    
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
        #print("\nKey error, moving on..")
        print(e)

    return tweets

def get_sinceID(user):
    conn=sqlite3.connect('data/database.db')
    c=conn.cursor()
    c.execute("SELECT sinceid FROM users WHERE userid=(?)",[user])
    result=c.fetchall()
    conn.commit()
    conn.close()

    return result

def set_sinceID(user, sinceid):
    conn=sqlite3.connect('data/database.db')
    c=conn.cursor()
    c.execute("UPDATE users SET sinceid = (?) WHERE userid = (?)", (sinceid, user))
    conn.commit()
    conn.close()


def get_tweets(token_dict, userid, GLOBALCOUNT):
    #print(token_dict)
    #parent_dir=os.path.dirname(os.path.abspath(__file__))
    #newdir="User-Tweets/%s" % (GLOBALCOUNT)
    #path = os.path.join(parent_dir, newdir)

    for user in tqdm(userid):

        since_var = get_sinceID(user)

        tweets = get_tweet_responses(
            token_dict['consumer_key'],
            token_dict['consumer_secret'],
            token_dict['access_token'],
            token_dict['access_token_secret'],
            user, num_tweets=10, since=since_var
        )

        if len(tweets)==0: #get_tweet_responses can return None
            continue
        else:
            df = pd.DataFrame(tweets)
            tweet_list_str = df['tweet_id'].to_list()
            int_list = list(map(int, tweet_list_str))
            set_sinceID(user, max(int_list))
            #df.to_csv('User-Tweets/%s/%s.csv' % (GLOBALCOUNT, user), index=False) #WRITE TO PICKLE
            global_count_list = [GLOBALCOUNT] * len(tweet_list_str)
            user_id_list = [user]* len(tweet_list_str)
            df['GLOBALCOUNT'] = global_count_list
            df['user'] = user_id_list

            if os.path.isfile('data/df.pkl'):
                loaded_df = pd.read_pickle('data/df.pkl')
                df = pd.concat([loaded_df, df], ignore_index=True, sort=False)

            df.to_pickle('data/df.pkl')


def get_tokens():
    token_arr = []
    with open('tokens.txt') as f:
        tokens = f.read().strip().split('\n')
        for token in tokens:
            consumer_key,consumer_secret,access_token,access_token_secret = token.split('|')
            token_arr.append(dict(consumer_key=consumer_key,consumer_secret=consumer_secret,access_token=access_token,access_token_secret=access_token_secret))

    return token_arr


def run_collection(GLOBALCOUNT, user_file=None):
    if user_file is None:
        user_file = 'users.csv'

    if os.path.exists('working-tokens.json'):
        tokens = json.load(open('working-tokens.json'))
    else:
        tokens = get_tokens()
        json.dump(tokens, open('working-tokens.json', 'w'))

    #with open(user_file) as f:
    #    userid = f.read().strip().split('\n')
    user_df = pd.read_csv('users.csv')
    userid = user_df['UserIDs'].apply(lambda x: str(x)).to_list()

    NUM_THREADS = len(tokens) #equivalent to num tokens
    userid_per_thread = ceil(len(userid) / len(tokens))

    threads = []
    for i in range(NUM_THREADS):
        token_dict = tokens[i]
        start = int(i * userid_per_thread)
        thread_userid = userid[start:start + userid_per_thread]
        thread = Thread(target=get_tweets, args=(token_dict, thread_userid, GLOBALCOUNT, ))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

   
