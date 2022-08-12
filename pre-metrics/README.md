### How to run (test)

- Tweets/Retweets: ```python get_re_tweets.py -u users/test.csv -t tokens/tokens.txt -n 100```
- Followees: ```python get_followees.py -u users/test.csv -t tokens/tokens.txt```
- Likes: ```python get_likes.py -u users/test.csv -t tokens/tokens.txt -n 100```

User assignment:
```python assign_treatments.py -u users/test.csv -m utils/media_political_twitter.csv -f data/test_followees_df.csv -l data/test_likes_df.csv -t data/test_tweets_df.csv -d 100```
