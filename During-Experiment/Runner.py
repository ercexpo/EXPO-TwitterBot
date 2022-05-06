import csv
import pandas as pd
from datetime import datetime, timedelta
from time import time, sleep
import json
from collections import defaultdict
from Reply_Generator import run_model
from Get_Tweets import run_collection
import os
from tqdm.auto import tqdm
import math

while True:
   
    sleep(28800 - time() % 28800)


    now = datetime.now()
    dt_string = now.strftime("%Y-%m-%d")
	
    #get-user-tweets
    run_collection(dt_string)

    with open('Database.txt',"r") as f:
        data = json.loads(f.read())

        for user in tqdm(os.listdir("User-Tweets")):
            df=pd.read_csv(user)

            #if user in treatment group then continue with the rest
            useridname=user.split(".csv")[0]
            
            if data[useridname][0][0]== False:
                continue

            #if user was replied to in the past 8 hours then continue
            previous_time=data[useridname][0][1]
            datetime_object = datetime.strptime(previous_time, '%m/%d/%y %H:%M:%S')

            a = datetime.datetime(now.year, now.month, now.day, now.hour, now.minute, now.second)
            b=datetime.datetime(datetime_object.year,datetime_object.month, datetime_object.day, datetime_object.hour, datetime_object.minute, datetime_object.second)

            c = a-b 

            hours = math.floor(c.total_seconds() / 3600)

            if hours < 8:
                continue


            tweettext=df['Full_text'].to_list()
            #if tweet text already present in replied tweets then continue

            
            for tweet in tweettext:
                #match Tweet Function

                if match == True:
                    generated_output=run_model(tweet)

                #     #post a reply

                else:
                    pass
            




