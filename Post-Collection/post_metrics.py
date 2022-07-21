from math import ceil
import tweepy as tw
import pandas as pd
from threading import Thread
from tqdm.auto import tqdm
import os
import json
from datetime import datetime
import os
from requests_oauthlib import OAuth1
import requests
from dotenv import load_dotenv


def returntweets(token_dict, userid, count):

    def get_tweet_responses(consumer_key, consumer_secret, access_token, access_token_secret, user, num_tweets):

        auth = tw.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        api = tw.API(auth, wait_on_rate_limit=True)

        #add sinceid here
        #open database, get sinceid as the max of the previous collected TwitterIDs and add as a parameter
        
       
        tweetsReq = tw.Cursor(api.user_timeline,
            user_id=user, count=num_tweets,
            exclude_replies=False, include_rts=True,
            tweet_mode='extended').items(num_tweets)
        tweets = []
        try:
            for tweet in tweetsReq:
                try:
                    full_text=tweet.retweeted_status.full_text
                except AttributeError:  # Not a Retweet
                    full_text=tweet.full_text
                tweets.append(dict(
                        full_text = full_text,
                        tweet_id=tweet.id_str,
                        screen_name=tweet.user.screen_name,
                        user_ID=user
                    ))
        except Exception as e:
            print(e)

        return tweets

    def getTweets(token_dict, userid, count):
        metalist=[]
        for user in tqdm(userid):

            

            tweets = get_tweet_responses(
                token_dict['consumer_key'],
                token_dict['consumer_secret'],
                token_dict['access_token'],
                token_dict['access_token_secret'],
                user, num_tweets=count
            )

            if len(tweets)==0:
                continue
            else:
                df = pd.DataFrame(tweets)
                metalist.append(df)
        return metalist

    return getTweets(token_dict,userid, count)

def returnlikes(token_dict, userid, count):
    def get_tweet_responses(consumer_key, consumer_secret, access_token, access_token_secret, user, num_tweets):
        auth = tw.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        api = tw.API(auth, wait_on_rate_limit=True)
        tweetsReq = tw.Cursor(api.get_favorites,
                user_id=user, count=num_tweets,
                tweet_mode='extended').items(num_tweets)
        tweets = []
        try:
            for tweet in tweetsReq:
                tweets.append(dict(
                        full_text = tweet.full_text,
                        tweet_id=tweet.id_str,
                        screen_name=tweet.user.screen_name,
                        Tweet_user_ID=tweet.user.id_str,
                        original_user_ID=user
                    ))
        except Exception as e:
            print(e)


        return tweets

    def getLikes(token_dict, listofusers,count):
        metalist=[]
        for user in tqdm(listofusers):
            tweets = get_tweet_responses(
                token_dict['consumer_key'],
                token_dict['consumer_secret'],
                token_dict['access_token'],
                token_dict['access_token_secret'],
                user, num_tweets=count
            )
            if len(tweets)==0:
                continue
            else:
                df = pd.DataFrame(tweets)
                metalist.append(df)
        return metalist
    return getLikes(token_dict,userid, count)

def followeelist(token_dict,listofusers):
    def get_tweet_responses(consumer_key, consumer_secret, access_token, access_token_secret, user):
        auth = tw.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        api = tw.API(auth, wait_on_rate_limit=True)
        tweetsReq = []
        try:
            for page in tw.Cursor(api.get_friend_ids,user_id=user,count=200).pages():
                tweetsReq.extend(page)


            tweets = []
        


            tweets.append(dict(
                Followers = tweetsReq,
                user_ID=user
            ))
        except Exception as e:
            print(e)


        return tweets

    def getFollowers(token_dict, listofusers):
        metalist=[]
        for user in tqdm(listofusers):
            tweets = get_tweet_responses(
                token_dict['consumer_key'],
                token_dict['consumer_secret'],
                token_dict['access_token'],
                token_dict['access_token_secret'],
                user
            )
            if len(tweets)==0:
                continue
            else:
                df = pd.DataFrame(tweets)
                metalist.append(df)
        return metalist
    return getFollowers(token_dict,listofusers)

def replytobot(token_dict,tweetid,botusername):
    now = datetime.now()
    DATE = now.strftime("%Y-%m-%d")

    def get_tweet_responses(consumer_key, consumer_secret, access_token, access_token_secret, tweetid, num_tweets, date, botusername):
        auth = tw.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        api = tw.API(auth, wait_on_rate_limit=True)

        name = botusername
        tweet_id=tweetid

        tweetsReq = tw.Cursor(api.search_tweets,
                q='to: '+name + " -filter:retweets",
                lang="en", until=date,
                result_type='recent', tweet_mode='extended').items(num_tweets)

        tweets = []
        try:
            for tweet in tweetsReq:
                if hasattr(tweet, 'in_reply_to_status_id_str') == False:
                    continue
                if (tweet.in_reply_to_status_id_str==tweet_id):
                    tweets.append(dict(
                        full_text = tweet.full_text,
                        BotTweetID=tweet_id,
                        tweet_id=tweet.id_str,
                        screen_name=tweet.user.screen_name,
                        user_id=tweet.user.id_str
                        ))
        except Exception as e:
            print(e)

        return tweets

    def getTweets(token_dict, tweetid, botusername):
        for tweet in tqdm(tweetid):
            tweets = get_tweet_responses(
                token_dict['consumer_key'],
                token_dict['consumer_secret'],
                token_dict['access_token'],
                token_dict['access_token_secret'],
                tweetid=tweet, num_tweets=20000, date=DATE, botusername=botusername
            )
            if len(tweets)==0:
                continue
            else:
                df = pd.DataFrame(tweets)
                return df
    return getTweets(token_dict, tweetid, botusername)

def linkclick(token_dict,tweetid ):
        metalist=[]
        for tweet in tweetid:

                try:

                        load_dotenv()

                        YOUR_TWEET_ID = '1549423851691319296'
                        #YOUR_TWEET_ID = tweet

                        url = 'https://api.twitter.com/2/tweets/{}?tweet.fields=public_metrics,non_public_metrics'.format(YOUR_TWEET_ID)

                        CONSUMER_KEY=token_dict['consumer_key']         
                        CONSUMER_SECRET=token_dict['consumer_secret']
                        ACCESS_TOKEN=token_dict['access_token']
                        ACCESS_SECRET=token_dict['access_token_secret']

                        headeroauth = OAuth1(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_TOKEN, ACCESS_SECRET, signature_type='auth_header')
                        r = requests.get(url, auth=headeroauth)




                        print(r)
                        print(r.json())
                        r=r.json()

                        clicks=r['data']['non_public_metrics']['url_link_clicks']
                        print(clicks)
                        d={'TweetID':tweet, 'LinkClicks': clicks}
                        df=pd.DataFrame.from_dict(d)
                        metalist.append(df)
                
                except Exception as e:
                        print(e)

        return metalist