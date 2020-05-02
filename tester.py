import tweepy
from datetime import date

consumer_key="5xSjIE64dtFQwM29rqcOAysxK"
consumer_secret="vCgfZ4HykNY3EGiq2NNyjlov9Als9NRdG9dSCcUPGKxCoqNwhN"

access_token="1249410623160475649-naT64yTvQOPSph7rBEGXrz3N5BNRj1"
access_token_secret="CFCmMQEwR223jLyO9kfvwoi48cZydE5PBRmS7PSgaPPwz"

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)

def get_all_received():
    messageData = api.list_direct_messages()
    #print(messageData)
    for words in messageData:
        if words.message_create.get(u'sender_id') != '1249410623160475649':
            text = words.message_create.get(u'message_data').get(u'text')
            id = words.message_create.get(u'sender_id')
            print(id)
            #add_dictionary(id, text)
            #fips = userbase.fips_from_text(text)
            #information = string_to_send(fips)
            #api.send_direct_message(id, test)
get_all_received()
