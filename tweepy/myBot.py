
import tweepy

print ("My Twitter bot")

consumer_key="5xSjIE64dtFQwM29rqcOAysxK"
consumer_secret="vCgfZ4HykNY3EGiq2NNyjlov9Als9NRdG9dSCcUPGKxCoqNwhN"

access_token="1249410623160475649-TbE0WxUsKb0C3uQsrXH8ykLc9mWgje"
access_token_secret="DqfR7rp2a9NICiLg8VbaHOytgkJVjaenwR0RkhVElGxuQ"

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)
