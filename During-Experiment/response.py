from profanity_check import predict, predict_prob
from transformers import AutoModelForCausalLM, AutoTokenizer
import torch
import pandas as pd
import re

stop_file = open("stopwords.txt", "r")
try:
    content = stop_file.read()
    stop_words = content.split(",")
finally:
    stop_file.close()


def is_faulty(original, response):
    
    # If original tweet and response generated are the same
    if original == response:
      return True
    
    # If generated response repeats itself
    i = (response + response).find(response, 1, -1)
    if i != -1:
      return True

    temp_response = response.lower()
    for word in set(temp_response.split()): 
      indexes = [w.start() for w in re.finditer(word, temp_response)]
      if len(indexes) >= 2 and word not in stop_words:
        return True

    # If empty response
    if response == '':
      return True

    # If using Reddit lingo in response
    reddit_lingo = ['upvote', 'downvote', 'downvoting', 'upvoting', 'reddit', 'up vote', 'down vote', 'subreddit', 'sub reddit', 'troll', 'trolling', 'japanese', 'reference']
    if any(word in response.lower() for word in reddit_lingo):
      return True

    # If string starts with a known problematic response: 
    options = ["I'm not sure if you're serious or not, but I'm going to go with the latter",
               "I'm not sure if you're serious or not, but I'm going to go with the former",
               "I'm not sure if you're serious or not, but I'm going to go with serious",
               "I'm not sure if you're serious or not, but I'm going to go with not serious",
               "I'm not sure if you're being serious or not, but I'm going to go with the latter",
               "I'm not sure if you're being serious or not, but I'm going to go with the former",
               "I'm not sure if you're being serious or not, but I'm going to go with serious",
               "I'm not sure if you're being serious or not, but I'm going to go with not serious",
               "I'm not sure if you're being serious or sarcastic",
               "I'm not sure if you're serious or sarcastic",
               "I'm not sure if you're serious",
               "I'm not sure if you're being serious",
               "I'm not sure if you're sarcastic",
               "I'm not sure if you're being sarcastic",
               ]
    for option in options:
      if response.startswith(option):
        return True

    # Check for offensive words
    if predict([response])[0] == 1:
      return True

    # No filters caught this response, so return False
    return False



def clean_tweet(tweet):
  tweet = re.sub("@[A-Za-z0-9_]+","", tweet)
  tweet = re.sub("#[A-Za-z0-9_]+","", tweet)
  tweet = re.sub(r"http\S+", "", tweet)
  tweet = re.sub(r"www.\S+", "", tweet)
  return tweet


#DIALO-GPT

def load_model():
    tokenizer = AutoTokenizer.from_pretrained("microsoft/DialoGPT-medium")
    model = AutoModelForCausalLM.from_pretrained("microsoft/DialoGPT-medium")

    return tokenizer, model


def run_model(tweets, tokenizer, model):
    #tweets is a list of tweet texts

    output = []
    count = 0
    for tweet in tweets:
        rgx = r"(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'\".,<>?«»“”‘’]))"
        checker = re.findall(rgx, tweet)
        checker_list = [u[0] for u in checker]
        if checker_list != []:
            output.append('Template Response TBD')
            continue

        cleaned_tweet = clean_tweet(tweet)
        new_user_input_ids = tokenizer.encode(cleaned_tweet + tokenizer.eos_token, return_tensors='pt')
        bot_input_ids = new_user_input_ids
        chat_history_ids = model.generate(bot_input_ids, max_length=1000, pad_token_id=tokenizer.eos_token_id, temperature=0.045, repetition_penalty=1.3)
        output.append(tokenizer.decode(chat_history_ids[:, bot_input_ids.shape[-1]:][0], skip_special_tokens=True))
  
        if is_faulty(cleaned_tweet, output[-1]):
            count += 1
            print(count, output[-1], cleaned_tweet)
            output[-1] = "Template Response TBD"

    #output is a list of responses the same length as tweets provided at input
    return output
