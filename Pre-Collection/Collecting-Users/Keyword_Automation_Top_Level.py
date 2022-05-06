from math import ceil
import tweepy as tw
import pandas as pd
from threading import Thread
from tqdm.auto import tqdm
import os
import json

DATE = "2022-05-05"

def get_tweet_responses(consumer_key, consumer_secret, access_token, access_token_secret, search_words, num_tweets, date):
    auth = tw.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tw.API(auth, wait_on_rate_limit=True)
    tweetsReq = tw.Cursor(api.search_tweets,
            q=search_words + " -filter:retweets",
            lang="en", until=date,
            result_type='recent', tweet_mode='extended').items(num_tweets)

    tweets = []
    for tweet in tweetsReq:
        if tweet.in_reply_to_status_id is not None:
            continue
        if tweet.user.verified == True:
            continue
        user=tweet.user.screen_name
        user=user.lower()
        if (user.find('bot') != -1):
            continue
        
        tweets.append(dict(
            full_text = tweet.full_text,
            tweet_id=tweet.id_str,
            screen_name=tweet.user.screen_name,
            user_id=tweet.user.id_str
        ))

    return tweets

def getTweets(token_dict, keywords):
    print(token_dict)
    for keyword in tqdm(keywords):
        tweets = get_tweet_responses(
            token_dict['consumer_key'],
            token_dict['consumer_secret'],
            token_dict['access_token'],
            token_dict['access_token_secret'],
            keyword, num_tweets=20000, date=DATE
        )
        pd.DataFrame(tweets).to_csv('keyword-results-top-1/%s.csv' % (keyword), index=False)

def getTokens():
    token_arr = []
    with open('tokens') as f:
        tokens = f.read().strip().split('\n')
        for token in tokens:
            consumer_key,consumer_secret,access_token,access_token_secret = token.split('|')
            try:
                get_tweet_responses(consumer_key, consumer_secret, access_token, access_token_secret, "NBA", 1, DATE)
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

    with open('keywords') as f:
        keywords = f.read().strip().split('\n')

    NUM_THREADS = len(tokens) #equivalent to num tokens
    KEYWORDS_PER_THREAD = ceil(len(keywords) / len(tokens))

    threads = []
    for i in range(NUM_THREADS):
        token_dict = tokens[i]
        start = int(i * KEYWORDS_PER_THREAD)
        thread_keywords = keywords[start: start + KEYWORDS_PER_THREAD]
        thread = Thread(target=getTweets, args=(token_dict, thread_keywords, ))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()