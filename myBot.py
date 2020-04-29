from userBase import userBase
import tweepy

consumer_key="5xSjIE64dtFQwM29rqcOAysxK"
consumer_secret="vCgfZ4HykNY3EGiq2NNyjlov9Als9NRdG9dSCcUPGKxCoqNwhN"

access_token="1249410623160475649-naT64yTvQOPSph7rBEGXrz3N5BNRj1"
access_token_secret="CFCmMQEwR223jLyO9kfvwoi48cZydE5PBRmS7PSgaPPwz"

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)

FILE_NAME = 'lastSeenId.txt'
#keeps track of the last seen id from all the mentions
def retrieve_last_seen_id(file_name):
    f_read = open(file_name, 'r')
    last_seen_id = int(f_read.read().strip())
    f_read.close()
    return last_seen_id
#stores teh new last seen id from mentions
def store_last_seen_id(last_seen_id, file_name):
    f_write = open(file_name, 'w')
    f_write.write(str(last_seen_id))
    f_write.close()
    return

def reply_to_tweets():
    last_seen_id = retrieve_last_seen_id(FILE_NAME)
    # everytime someone @s us, makes a list
    mentions = api.mentions_timeline(last_seen_id, tweet_mode='extended')
    #for all the mentions, respind
    for mention in reversed(mentions):
        #just to print on console(not essential)
        print(str(mention.id) + ' - ' + mention.full_text)
        #puts the last see ids onto the file
        last_seen_id = mention.id
        store_last_seen_id(last_seen_id, FILE_NAME)
        #respond to the file using userbase
        information = userbase.fips_from_text(mention.full_text.lower())
        api.update_status('@' + mention.user.screen_name + ' ' + information, mention.id)

#send dm to all followers
def send_direct_messages():
    #subscriberList = api.followers_ids(1249410623160475649)
    for id in allMembers:
        message = allMembers[id]
        api.send_direct_message(id, userbase.fips_from_text(message))

def add_dictionary(id, message):
    if id not in allMembers:
        allMembers[id] = message
    if not userbase.if_user_exists(id):
        userbase.new_user(id, location=message)
    return allMembers

#gets all recieved dms text
def get_all_received():
    messageData = api.list_direct_messages()
    #print(messageData)
    for words in messageData:
        if words.message_create.get(u'sender_id') != '1249410623160475649':
            text = words.message_create.get(u'message_data').get(u'text')
            id = words.message_create.get(u'sender_id')
            add_dictionary(id, text)
            api.send_direct_message(id, userbase.fips_from_text(text))

if __name__ == '__main__':
    get_all_received()
    send_direct_messages()
    #need to only run this everyday
    #send_direct_messages()
