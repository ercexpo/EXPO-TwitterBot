import csv
import pandas as pd
from datetime import datetime, timedelta
from time import time, sleep
import json
from collections import defaultdict
import response
import random
from get_tweets import run_collection
from match_tweet import match_keywords
import os
from tqdm.auto import tqdm
import math
import sqlite3
from post_response import run_posting
import torch
import sys
import numpy as np

def check_treatment(useridname, db_file):
    conn=sqlite3.connect(db_file)
    c=conn.cursor()
    c.execute("SELECT Treatment FROM users WHERE userid = (?)", [useridname])
    result=c.fetchone()

    return result

def get_previous_time(useridname, db_file):
    conn=sqlite3.connect(db_file)
    c=conn.cursor()
    c.execute("SELECT last_replied FROM users WHERE userid = (?)", [useridname])
    result=c.fetchone()

    return result[0]

def get_hours(useridname, db_file):
        #if user was replied to in the past 24 hours then continue
        previous_time=get_previous_time(useridname, db_file)
        datetime_object = datetime.strptime(previous_time, '%Y/%m/%d %H:%M:%S')

        a = datetime(now.year, now.month, now.day, now.hour, now.minute, now.second)
        b=datetime(datetime_object.year,datetime_object.month, datetime_object.day, datetime_object.hour, datetime_object.minute, datetime_object.second)

        c = a-b

        hours = math.floor(c.total_seconds() / 3600)

        return hours

'''
def obtain_treatment_dict(user_file):
    tdict = defaultdict()
    u_df = pd.read_csv(userfile)
    for i in range(len(u_df)):
        username = u_df.iloc[i]['UserIDs']
        treatment = u_df.iloc[i]['treatment']
        tdict[username] = treatment
'''

#Prepare response generator model/files
response_templates, news_templates, sports_df, entertainment_df, lifestyle_df = response.load_all_files()
tokenizer, model = response.load_model()

# Initialize variable indicating global iteration of the code
GLOBALCOUNT = 0

if __name__ == "__main__":
    try:
        user_file = sys.argv[1]
        db_file = sys.argv[2]
    except:
        print("Please provide a user file and database file as input")


    while True:

        GLOBALCOUNT += 1
   

        now = datetime.now()
        dt_string = now.strftime("%Y-%m-%d")

        run_collection(GLOBALCOUNT, user_file) 
        #treatment_dict = obtain_treatment_dict(user_file)

        post_tweets_dump=[]
        userid=[]
        originalTweet=[]
        tweetid=[]
        timestamp=[]

        data_df = pd.read_pickle('data/df.pkl')
        print(data_df)

        sub_df = data_df[data_df['GLOBALCOUNT'] == GLOBALCOUNT]
        users_list = sub_df['user'].to_list()

        done_check = defaultdict(lambda: False)

        for useridname in tqdm(np.unique(users_list)):
            df = sub_df[sub_df['user'] == useridname]

            #if user in treatment group then continue with the rest
        
            #if check_treatment(useridname)== False: #might need to change this to a string
            #    continue

            hours=get_hours(useridname, db_file)
            #if user was replied to in the past 24 hours then continue


            if GLOBALCOUNT != 1:
                if hours < 24:
                    continue

            tweet_text=df['full_text'].to_list()
            tweet_ids=df['tweet_id'].to_list()
        

            for tweet, ids in zip(tweet_text,tweet_ids):
                if done_check[ids]:
                    continue
                #match tweet
                interact, topic = match_keywords(tweet)
                #print(interact, topic, tweet)

                if interact:
                    generated_responses = response.run_model([tweet], tokenizer, model, response_templates)
                    generated_output=response.append_url(topic, generated_responses, news_templates, sports_df, entertainment_df, lifestyle_df)
                    post_tweets_dump.append(generated_output)
                    originalTweet.append(tweet)
                    tweetid.append(ids)
                    userid.append(useridname)
                    now = datetime.now()
                    dt_string = now.strftime("%Y/%m/%d %H:%M:%S")
                    timestamp.append(dt_string)
                    done_check[ids] = True

                else:
                    continue
        
        replydict={'UserID': userid, 'TweetID': tweetid, 'Original_Tweet': originalTweet, 'Reply': post_tweets_dump, 'TimeStamp': timestamp}
        reply_df=pd.DataFrame.from_dict(replydict)

        print(reply_df)

        #run_posting(reply_df, dt_string, treatment_dict) #UNCOMMENT

        #sleep(28800 - time() % 28800) #UNCOMMENT

        break

    



