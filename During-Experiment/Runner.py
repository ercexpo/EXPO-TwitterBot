import csv
import pandas as pd
from datetime import datetime, timedelta
from time import time, sleep
import json
from collections import defaultdict
import response
import random
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

response_templates, news_templates, sports_df, entertainment_df, lifestyle_df = response.load_all_files()

tokenizer, model = response.load_model()

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
    timestamp=[]

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
            interact, topic = matchKeywords(tweet)

            if interact == True:
                generated_responses = response.run_model(list(tweet), tokenizer, model, response_templates)
                generated_output=response.append_url(topic, generated_responses, news_templates, sports_df, entertainment_df, lifestyle_df)
                post_tweets_dump.append(generated_output)
                originalTweet.append(tweet)
                tweetid.append(ids)
                userid.append(useridname)
                now = datetime.now()
                dt_string = now.strftime("%Y/%m/%d %H:%M:%S")
                timestamp.append(dt_string)
                

            else:
                continue
        
    replydict={'UserID': userid, 'TweetID': tweetid, 'Original_Tweet': originalTweet, 'Reply': post_tweets_dump, 'TimeStamp': timestamp}
    df1=pd.DataFrame.from_dict(replydict)
    df1.to_csv('Tweets_to_be_posted.csv')


