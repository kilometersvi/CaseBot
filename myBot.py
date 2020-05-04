from userbase import userbase
from data import data
import datetime
import tweepy

consumer_key="5xSjIE64dtFQwM29rqcOAysxK"
consumer_secret="vCgfZ4HykNY3EGiq2NNyjlov9Als9NRdG9dSCcUPGKxCoqNwhN"

access_token="1249410623160475649-naT64yTvQOPSph7rBEGXrz3N5BNRj1"
access_token_secret="CFCmMQEwR223jLyO9kfvwoi48cZydE5PBRmS7PSgaPPwz"

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth, wait_on_rate_limit=True)


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

def retrieve_last_dm():
    f_read = open('lastSeenDM.txt', 'r')
    count = int(f_read.read())
    f_read.close()
    return count

def new_last_dm(count):
    f_write = open('lastSeenDM.txt', 'w')
    f_write.write(str(count))
    f_write.close()
    return count

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
    today = datetime.date.today()
    yesterday = datetime.date.today() - datetime.timedelta(days = 1)
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
    info_string = "Total Number of Cases: " + str(total_cases) + "\n" + "Total Number of Deaths: " + str(total_deaths) + "\n" + "New Cases Today: " + str(new_cases) + "\n" +"New Deaths Today: " + str(new_deaths)
    return info_string

#send dm to all followers
def send_dms():
    messageData = api.list_direct_messages()
    check_list = []
    for words in messageData:
        if words.message_create.get(u'sender_id') != '1249410623160475649':
            id = words.message_create.get(u'sender_id')
            if userbase.if_user_exists(id):
                check_list.append(id)
                if id not in check_list:
                    dict = userbase.get(id)
                    fips = dict[location]
                    information = string_to_send(fips)
                    api.send_direct_message(id, information)

#returns if need to send to dm right away
def add_dictionary(id, message):
    fips = userBase.fips_from_text(message)
    if not userbase.if_user_exists(id):
        userbase.new_user(id, location=fips)
        return True
    return False

#gets all recieved dms text
def get_all_received_dms():
    messageData = api.list_direct_messages()
    count = retrieve_last_dm()
    inCount = 0
    for words in reversed(messageData):
        if words.message_create.get(u'sender_id') != '1249410623160475649':
            if inCount >= count:
                text = words.message_create.get(u'message_data').get(u'text')
                id = words.message_create.get(u'sender_id')
                text = text.lower()
                if "stop" in text:
                    userbase.remove_user(id)
                elif "subscribe " in text:
                    text.replace("subscribe ", '')
                    add_dictionary(id, text)
                    api.send_direct_message(id, "We will keep you updated, reply 'stop' to quit")
                    if " county" in text:
                        text.replace(" county", '')
                    fips = userbase.fips_from_text(text)
                    information = string_to_send(fips)
                    api.send_direct_message(id, information)
                else:
                    if " county" in text:
                        text.replace(" county", '')
                    fips = userbase.fips_from_text(text)
                    information = string_to_send(fips)
                    api.send_direct_message(id, information)
                inCount += 1
            else:
                inCount += 1
    new_last_dm(inCount)


if __name__ == '__main__':
    reply_to_tweets()
    get_all_received_dms()
    send_dms()
