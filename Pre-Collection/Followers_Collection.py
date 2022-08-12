from math import ceil
import tweepy as tw
import pandas as pd
from threading import Thread
from tqdm.auto import tqdm
import os
import json


df1=pd.read_csv('Userlist.csv')
df1=df1['0']
print(df1.head())
listofusers=df1.to_list()
print(len(listofusers))

# for i in range(len(listofusers)):
#     listofusers[i]=listofusers[i].split('.')[0]


DATE = "2022-08-02"

def get_tweet_responses(consumer_key, consumer_secret, access_token, access_token_secret, keyword):
    auth = tw.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tw.API(auth, wait_on_rate_limit=True)
    tweetsReq = []
    try:
        for page in tw.Cursor(api.get_friend_ids,user_id=keyword,count=200).pages():
            tweetsReq.extend(page)


        tweets = []
    


        tweets.append(dict(
            Followers = tweetsReq,
            user_ID=keyword
        ))
    except Exception as e:
        print(e)


    return tweets

def getFollowers(token_dict, listofusers):
    print(token_dict)
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
            pd.DataFrame(tweets).to_csv('User-Followers/%s.csv' % (user), index=False)

def getTokens():
    token_arr = []
    with open('tokens') as f:
        tokens = f.read().strip().split('\n')
        for token in tokens:
            consumer_key,consumer_secret,access_token,access_token_secret = token.split('|')
            try:
                get_tweet_responses(consumer_key, consumer_secret, access_token, access_token_secret, 4148542639, 1, DATE)
                token_arr.append(dict(consumer_key=consumer_key,consumer_secret=consumer_secret,access_token=access_token,access_token_secret=access_token_secret))
            except Exception as e:
                continue
    return (token_arr)


if __name__ == '__main__':
    if os.path.exists('working-tokens.json'):
        tokens = json.load(open('working-tokens.json'))
    else:
        tokens = getTokens()
        json.dump(tokens, open('working-tokens.json', 'w'))



    NUM_THREADS = len(tokens) #equivalent to num tokens
    KEYWORDS_PER_THREAD = ceil(len(listofusers) / len(tokens))

    threads = []
    for i in range(NUM_THREADS):
        token_dict = tokens[i]
        start = int(i * KEYWORDS_PER_THREAD)
        thread_keywords = listofusers[start: start + KEYWORDS_PER_THREAD]
        thread = Thread(target=getFollowers, args=(token_dict, thread_keywords, ))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()