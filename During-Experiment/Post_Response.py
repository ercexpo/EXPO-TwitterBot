from math import ceil
import tweepy as tw
import pandas as pd
from threading import Thread
from tqdm.auto import tqdm
import os
import json
import sqlite3
from datetime import datetime

def UpdateLastReplied(userid):
    now = datetime.now()
    dt_string = now.strftime("%Y/%m/%d %H:%M:%S")

    conn=sqlite3.connect('database.db')
    c=conn.cursor()
    c.execute("UPDATE users SET last_replied = (?) WHERE userid = (?)", dt_string, userid)
    print("Set sinceID")
    conn.commit()
    conn.close()





def postTweets(token_dict,replies,tweetids,userIDs):
    for reply, id, user in tqdm(zip(replies,tweetids, userIDs)):
            consumer_key=token_dict['consumer_key']
            consumer_secret=token_dict['consumer_secret'],
            access_token=token_dict['access_token'],
            access_token_secret=token_dict['access_token_secret']

            auth = tw.OAuthHandler(consumer_key, consumer_secret)
            auth.set_access_token(access_token, access_token_secret)
            api = tw.API(auth, wait_on_rate_limit=True)

            api.update_status(status = reply, in_reply_to_status_id = id , auto_populate_reply_metadata=True)
            UpdateLastReplied(user)

#update time
    

def getTokens():
    token_arr = []
    with open('tokens') as f:
        tokens = f.read().strip().split('\n')
        for token in tokens:
            consumer_key,consumer_secret,access_token,access_token_secret = token.split('|')
            token_arr.append(dict(consumer_key=consumer_key,consumer_secret=consumer_secret,access_token=access_token,access_token_secret=access_token_secret))

    return (token_arr)



def run_posting():
    if os.path.exists('working-tokens.json'):
        tokens = json.load(open('working-tokens.json'))

    df=pd.read_csv('Tweets_to_be_posted.csv')
    replytweets=df['Reply'].to_list()
    tweetIDs=df['TweetID'].to_list()
    userIDs=df['UserIDs'].to_list()

        


    NUM_THREADS = len(tokens) #equivalent to num tokens
    replytweets_PER_THREAD = ceil(len(replytweets) / len(tokens))
    tweetIDs_PER_THREAD= ceil(len(tweetIDs) / len(tokens))
    userIDs_PER_THREAD= ceil(len(userIDs) / len(tokens))

    threads = []
    for i in range(NUM_THREADS):
        token_dict = tokens[i]
        start = int(i * replytweets_PER_THREAD)
        start1 = int(i * tweetIDs_PER_THREAD)
        start2 = int(i * userIDs_PER_THREAD)
        thread_replies = replytweets[start: start + replytweets_PER_THREAD]
        thread_tweetIDs = tweetIDs[start1: start1 + tweetIDs_PER_THREAD]
        thread_userIDs = userIDs[start2: start2 + userIDs_PER_THREAD]

        thread = Thread(target=postTweets, args=(token_dict, thread_replies,thread_tweetIDs,thread_userIDs, ))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()