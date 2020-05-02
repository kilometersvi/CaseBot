from userBase import userBase
from data import data
from datetime import date
import tweepy

consumer_key="5xSjIE64dtFQwM29rqcOAysxK"
consumer_secret="vCgfZ4HykNY3EGiq2NNyjlov9Als9NRdG9dSCcUPGKxCoqNwhN"

access_token="1249410623160475649-naT64yTvQOPSph7rBEGXrz3N5BNRj1"
access_token_secret="CFCmMQEwR223jLyO9kfvwoi48cZydE5PBRmS7PSgaPPwz"

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)

allMembers = {}

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
        fips = userbase.fips_from_text(mention.full_text.lower())
        information = string_to_send(fips)
        api.update_status('@' + mention.user.screen_name + ' ' + information, mention.id)

def string_to_send(fips):
    today = date.today()
    yesterday = today - timedelta(days = 1)
    information = data.get_county_data(fips, today)
    yesterday_info = data.get_county_data(fips, yesterday)
    total_cases = information[today][0]
    total_deaths = information[today][1]
    new_cases = information[today][0] - yesterday_info[yesterday][0]
    if new_cases < 0:
        new_cases = 0
    new_deaths = information[today][1] - yesterday_info[yesterday][1]
    if new_deaths < 0:
        new_deaths = 0
    info_string = "Total Number of Cases: " + total_cases + "\n" +
                     "Total Number of Deaths: " + total_deaths + "\n" +
                     "New Cases Today: " + new_cases + "\n" +
                     "New Deaths Today: " + new_deaths
    return info_string


#send dm to all followers
def send_direct_messages():
    #subscriberList = api.followers_ids(1249410623160475649)
    for id in allMembers:
        fips = allMembers[id]
        information = string_to_send(fips)
        api.send_direct_message(id, information)

def add_dictionary(id, message):
    fips = userBase.fips_from_text(message)
    if id not in allMembers:
        allMembers[id] = fips
    if not userbase.if_user_exists(id):
        userbase.new_user(id, location=fips)
        return true
    return false

#gets all recieved dms text
def get_all_received():
    messageData = api.list_direct_messages()
    #print(messageData)
    for words in messageData:
        if words.message_create.get(u'sender_id') != '1249410623160475649':
            text = words.message_create.get(u'message_data').get(u'text')
            id = words.message_create.get(u'sender_id')
            in_bool = add_dictionary(id, text)
            if in_bool:
                fips = userbase.fips_from_text(text)
                information = string_to_send(fips)
                api.send_direct_message(id, information)

if __name__ == '__main__':
    get_all_received()
    send_direct_messages()
    #need to only run this everyday
    #send_direct_messages()
