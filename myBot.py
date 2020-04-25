import tweepy

print ("My Twitter bot")

consumer_key="5xSjIE64dtFQwM29rqcOAysxK"
consumer_secret="vCgfZ4HykNY3EGiq2NNyjlov9Als9NRdG9dSCcUPGKxCoqNwhN"

access_token="1249410623160475649-naT64yTvQOPSph7rBEGXrz3N5BNRj1"
access_token_secret="CFCmMQEwR223jLyO9kfvwoi48cZydE5PBRmS7PSgaPPwz"

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)

subscriberList = api.followers_ids(1249410623160475649)

for id in subscriberList:
    api.send_direct_message(id, "Oh what up")

api.get_direct_message()

#api.update_status(status='Trying update')
#api.geo_id(id)
