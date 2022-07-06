from math import ceil
import tweepy as tw
import pandas as pd
from threading import Thread
from tqdm.auto import tqdm
import os
import json
import sqlite3
from datetime import datetime

def getinfo(response):
    responsetweet_id=response.id_str
    original_tweet_id=response.in_reply_to_status_id_str
    original_user_id=response.in_reply_to_user_id_str
    bot_id=response.user.id_str


def UpdateLastReplied(userid):
    now = datetime.now()
    dt_string = now.strftime("%Y/%m/%d %H:%M:%S")

    conn=sqlite3.connect('database.db')
    c=conn.cursor()
    c.execute("UPDATE users SET last_replied = (?) WHERE userid = (?)", (dt_string, userid))
    print("Set Last Replied")
    conn.commit()
    conn.close()


#need to figure out how to get the specific bot account to reply to the user assigned to it. First will have to validate keys, then grant
#the keys access to post via the 3 bot accounts


def postTweets(token_dict,replies,tweetids,userIDs):
    responsetweet_id=[]
    original_tweet_id=[]
    original_user_id=[]
    bot_id=[]
    #print(replies, tweetids, userIDs)
    for reply, id, user in tqdm(zip(replies,tweetids, userIDs)):
            print(reply, id, user)
            consumer_key=token_dict['consumer_key']
            consumer_secret=token_dict['consumer_secret']
            access_token=token_dict['access_token']
            access_token_secret=token_dict['access_token_secret']

            auth = tw.OAuthHandler(consumer_key, consumer_secret)
            auth.set_access_token(access_token, access_token_secret)
            api = tw.API(auth, wait_on_rate_limit=True)

            response=api.update_status(status = reply, in_reply_to_status_id = id , auto_populate_reply_metadata=True)
            responsetweet_id.append(response.id_str)
            original_tweet_id.append(response.in_reply_to_status_id_str)
            original_user_id.append(response.in_reply_to_user_id_str)
            bot_id.append(response.user.id_str)
            UpdateLastReplied(user)
        
    dict = {'responsetweet_id': responsetweet_id, 'original_tweet_id': original_tweet_id, 'original_user_id': original_user_id, 'bot_id': bot_id } 
    df = pd.DataFrame(dict)
    now = datetime.now()
    dt_string = now.strftime("%Y/%m/%d %H:%M:%S")
    df.to_csv('%s.csv' % ('Reply1'), index=False) #WRITE TO PICKLE

#update time
    

def getTokens():
    token_arr = []
    with open('tokens') as f:
        tokens = f.read().strip().split('\n')
        for token in tokens:
            consumer_key,consumer_secret,access_token,access_token_secret = token.split('|')
            token_arr.append(dict(consumer_key=consumer_key,consumer_secret=consumer_secret,access_token=access_token,access_token_secret=access_token_secret))

    return (token_arr)



def run_posting(timestamp):
    if os.path.exists('working-tokens.json'):
        tokens = json.load(open('working-tokens.json'))

    df=pd.read_csv('tweets_to_be_posted/temp.csv') #READ TO PICKLE
    #print(df.columns)
    replytweets=df['Reply'].to_list()
    tweetIDs=df['TweetID'].to_list()
    userIDs=df['UserID'].to_list()



        


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
