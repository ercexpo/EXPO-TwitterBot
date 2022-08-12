from math import ceil
import tweepy as tw
import pandas as pd
from threading import Thread
from tqdm.auto import tqdm
import os
import json
import geostring as geo
import spacy
from spacy_langdetect import LanguageDetector
from spacy.language import Language
from shutil import get_terminal_size 

DATE = "2022-07-25"

def get_tweet_responses(consumer_key, consumer_secret, access_token, access_token_secret, keyword, num_tweets, date):
    auth = tw.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tw.API(auth, wait_on_rate_limit=True)
    tweetsReq = tw.Cursor(api.user_timeline,
            user_id=keyword, count=10,
            exclude_replies=True, include_rts=False,
            tweet_mode='extended').items(num_tweets)

    tweets = []
    try:
        for tweet in tweetsReq:
            x=tweet.user.location
            x=geo.resolve(x)
            x=list(x.items())
            country=x[2][1]
            
            if '?' in country:
                    temp=country.split('?')
                    country=temp[1]

            else:
                    pass

            if country == 'united states':
                    pass
            else:
                    break
            
            nlp = spacy.load('en_core_web_sm')  # 1
            @Language.factory('language_detector')
            def language_detector(nlp, name):
                return LanguageDetector()
            nlp.add_pipe('language_detector', last=True)
            text_content = tweet.full_text
            doc = nlp(text_content)
            detect_language = doc._.language
            if detect_language['language'] != 'en':
                print(detect_language['language'])
                break


            tweets.append(dict(
                full_text = tweet.full_text,
                tweet_id=tweet.id_str,
                screen_name=tweet.user.screen_name,
                user_ID=keyword
            ))
    except Exception as e:
        print(e)

    return tweets

def getTweets(token_dict, keywords):
    print(token_dict)
    for keyword in tqdm(keywords):
        tweets = get_tweet_responses(
            token_dict['consumer_key'],
            token_dict['consumer_secret'],
            token_dict['access_token'],
            token_dict['access_token_secret'],
            keyword, num_tweets=10, date=DATE
        )
        if len(tweets)==0:
            continue
        else:
            pd.DataFrame(tweets).to_csv('User-Tweets/%s.csv' % (keyword), index=False)

def getTokens():
    token_arr = []
    with open('tokens') as f:
        tokens = f.read().strip().split('\n')
        for token in tokens:
            consumer_key,consumer_secret,access_token,access_token_secret = token.split('|')
            try:
                get_tweet_responses(consumer_key, consumer_secret, access_token, access_token_secret, 44201535, 1, DATE)
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