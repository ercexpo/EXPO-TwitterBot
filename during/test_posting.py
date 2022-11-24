from math import ceil
import tweepy as tw
import pandas as pd
from threading import Thread
from tqdm.auto import tqdm
import os
import json
import sqlite3
from datetime import datetime
from collections import defaultdict
from queue import Queue
import argparse

def getinfo(response):
    responsetweet_id=response.id_str
    original_tweet_id=response.in_reply_to_status_id_str
    original_user_id=response.in_reply_to_user_id_str
    bot_id=response.user.id_str


def update_last_replied(userid, db_file):
    now = datetime.now()
    dt_string = now.strftime("%Y/%m/%d %H:%M:%S")

    conn=sqlite3.connect(db_file)
    c=conn.cursor()
    c.execute("UPDATE users SET last_replied = (?) WHERE userid = (?)", (dt_string, userid))
    conn.commit()
    conn.close()



def post_tweets(token_dict,replies,tweetids,userIDs , q):
    responsetweet_id=[]
    original_tweet_id=[]
    original_user_id=[]
    bot_id=[]
    dt_strings = []

    #print(replies, tweetids, userIDs)

    done_check = defaultdict(lambda: False)

    for reply, id, user in tqdm(zip(replies,tweetids, userIDs)):
        if done_check[id]:
            continue
        #print(reply, id, user)
        consumer_key=token_dict['consumer_key']
        consumer_secret=token_dict['consumer_secret']
        access_token=token_dict['access_token']
        access_token_secret=token_dict['access_token_secret']

        
        auth = tw.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        api = tw.API(auth, wait_on_rate_limit=True)

        try:
            response=api.update_status(status = reply, in_reply_to_status_id = id , auto_populate_reply_metadata=True)
            # responsetweet_id.append(response.id_str)
            # original_tweet_id.append(response.in_reply_to_status_id_str)
            # original_user_id.append(response.in_reply_to_user_id_str)
            # bot_id.append(response.user.id_str)
            # dt_strings.append(datetime.now().strftime("%Y/%m/%d %H:%M:%S"))
            # update_last_replied(user, db_file)
            # done_check[id] = True
        except Exception as e:
            print(e)
            print("Problematic post_response keys ->")
            print(consumer_key, consumer_secret, access_token, access_token_secret)

        
    dict = {'responsetweet_id': responsetweet_id, 'original_tweet_id': original_tweet_id, 'original_user_id': original_user_id, 'bot_id': bot_id, "time": dt_strings} 
    q.put(dict)


def get_tokens(token_file):
    token_arr = []
    with open(token_file) as f:
        tokens = f.read().strip().split('\n')
        for token in tokens:
            consumer_key,consumer_secret,access_token,access_token_secret = token.split(',') ## token.split('|')
            token_arr.append(dict(consumer_key=consumer_key,consumer_secret=consumer_secret,access_token=access_token,access_token_secret=access_token_secret))

    return token_arr


def run_posting(token_file):
    tokens = get_tokens(token_file)

    replytweets=["Test"]*54
    tweetIDs=['1595697064888832001']*54
    userIDs=['']*54
    print(replytweets)
    print(tweetIDs)
    print(userIDs)

    NUM_THREADS = len(tokens) #equivalent to num tokens
    replytweets_PER_THREAD = ceil(len(replytweets) / len(tokens))
    tweetIDs_PER_THREAD= ceil(len(tweetIDs) / len(tokens))
    userIDs_PER_THREAD= ceil(len(userIDs) / len(tokens))

    res_q = Queue()

    threads = []
    for i in range(NUM_THREADS):
        token_dict = tokens[i]
        start = int(i * replytweets_PER_THREAD)
        start1 = int(i * tweetIDs_PER_THREAD)
        start2 = int(i * userIDs_PER_THREAD)
        thread_replies = replytweets[start: start + replytweets_PER_THREAD]
        thread_tweetIDs = tweetIDs[start1: start1 + tweetIDs_PER_THREAD]
        thread_userIDs = userIDs[start2: start2 + userIDs_PER_THREAD]

        thread = Thread(target=post_tweets, args=(token_dict, thread_replies,thread_tweetIDs,thread_userIDs, res_q))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    # rdf_pkl_file = 'data/' + user_file.split('users/')[1].split('.csv')[0] + '_replies_df.pkl'
    # while not res_q.empty():
    #     res = res_q.get()
    #     df = pd.DataFrame(res)
    #     if os.path.isfile(rdf_pkl_file):
    #         loaded_df = pd.read_pickle(rdf_pkl_file)
    #         df = pd.concat([loaded_df, df], ignore_index=True, sort=False)

        # df.to_pickle(rdf_pkl_file)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-u", "--user_file", help="Path to user file", type=str, required=False)
    parser.add_argument("-db", "--db_file", help="Path to database file", type=str, required=False)
    parser.add_argument("-t", "--token_file", help="Path to token file", type=str, required=True)
    parser.add_argument("-c", "--control_bool", help="Boolean indicating whether this is a control group experiment", type=str, required=False)
    parser.add_argument("-p", "--post_bool", help="Boolean indicating whether we should post generated responses", type=str, required=False)

    args = parser.parse_args()
    token_file = args.token_file

    run_posting(token_file)