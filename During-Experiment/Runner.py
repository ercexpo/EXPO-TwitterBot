import csv
import pandas as pd
from datetime import datetime, timedelta
from time import time, sleep
import json
from collections import defaultdict
from response import run_model
from Get_Tweets import run_collection
from match_tweet import matchKeywords
import os
from tqdm.auto import tqdm
import math
import sqlite3

def checkTreatment(useridname):
    conn=sqlite3.connect('database.db')
    c=conn.cursor()
    c.execute("SELECT Treatment FROM users WHERE userid = (?)", useridname)
    result=c.fetchone()

    return result

def getPrevioustime(useridname):
    conn=sqlite3.connect('database.db')
    c=conn.cursor()
    c.execute("SELECT last_replied FROM users WHERE userid = (?)", useridname)
    result=c.fetchone()

    return result



while True:
   
    sleep(28800 - time() % 28800)


    now = datetime.now()
    dt_string = now.strftime("%Y-%m-%d")
	
    #get-user-tweets
    run_collection(dt_string)

    post_tweets_dump=[]
    userid=[]
    originalTweet=[]
    tweetid=[]

    for user in tqdm(os.listdir("User-Tweets")):
        df=pd.read_csv(user)

        #if user in treatment group then continue with the rest
        useridname=user.split(".csv")[0]
        
        if checkTreatment(useridname)== False:
            continue

        #if user was replied to in the past 24 hours then continue
        previous_time=getPrevioustime(useridname)
        datetime_object = datetime.strptime(previous_time, '%m/%d/%y %H:%M:%S')

        a = datetime.datetime(now.year, now.month, now.day, now.hour, now.minute, now.second)
        b=datetime.datetime(datetime_object.year,datetime_object.month, datetime_object.day, datetime_object.hour, datetime_object.minute, datetime_object.second)

        c = a-b 

        hours = math.floor(c.total_seconds() / 3600)

        if hours < 24:
            continue


        tweettext=df['Full_text'].to_list()
        tweetids=df['Tweet_id'].to_list()
        #if tweet text already present in replied tweets then continue

        
        for tweet, ids in zip(tweettext,tweetids):
            #match Tweet Function

            if matchKeywords(tweet) == True:
                generated_output=run_model(tweet)
                post_tweets_dump.append(generated_output)
                originalTweet.append(tweet)
                tweetid.append(ids)
                userid.append(useridname)

            else:
                continue
        
    replydict={'UserID': userid, 'TweetID': tweetid, 'Original_Tweet': originalTweet, 'Reply': post_tweets_dump }
    df1=pd.DataFrame.from_dict(replydict)
    df1.to_csv('Tweets_to_be_posted.csv')



