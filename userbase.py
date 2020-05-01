from sql import db
from data import data

class userbase:
    log = False
    table_twitter = "users_twitter"
    table_reddit = "users_reddit"

    sql_user = "miles"
    sql_pass = "shrek5"
    '''
    user data:
        id (string) - id
        location (integer) - FIPS code
        update_pref (string) - how often user wants to receive message
        min_days_between_msg (integer) - hard limiter to prevent spam
        last_date_messaged (date) - var slot for last date a message was sent to user, not within scope of this script to maintain
    use guide:
        - create/set data with the same type as specified above
        - run create_tables when setting up on new database. only needs to be run once on machine to permanently make sql tables.
        - location param: if entered by county, string is produced by "{county}, {state}".format() eg orange county in california is accessed by "Orange, California"
        - when setting location, call fips_from_text to get fips from "{county}, {state}".format() string
        - county name does not include County at end e.g. Orange County is listed as Orange
        - state names are full eg CA is listed as California, not CA
        - date in format: YYYY-MM-DD
    '''

    def create_usertables(): #only run on new database
        connection = db.create_connection(data.sql_addr, userbase.sql_user, userbase.sql_pass)
        db.run(connection,"Create database if not exists "+data.db_name)
        db.commit(connection)
        connection.close()
        connection = db.create_connection(data.sql_addr, userbase.sql_user, userbase.sql_pass, data.db_name)
        db.run(connection,"Create table "+table_twitter+" (id VARCHAR(64) NOT NULL UNIQUE, location MEDIUMINT DEFAULT 0, update_pref VARCHAR(64) DEFAULT 'daily', min_days_between_msg MEDIUMINT DEFAULT 0, last_date_messaged DATE)")
        db.run(connection,"Create table "+table_reddit+" (id VARCHAR(64) NOT NULL UNIQUE, location MEDIUMINT DEFAULT 0, update_pref VARCHAR(64) DEFAULT 'daily', min_days_between_msg MEDIUMINT DEFAULT 0, last_date_messaged DATE)")
        db.commit(connection)
        connection.close()
        if userbase.log:
            print("[SETUP] tables created on ",data.db_name)

    def table_from_param(param): #ignore, only used by these functions
        table="unk"
        if param=="twitter":
            table=userbase.table_twitter
        elif param=="reddit":
            table=userbase.table_reddit
        else:
            print("[IMPLEMENTERROR] unknown platform")
            return False
        return table

    def fips_from_text(location):
        return data.fips_from_text(location)

    def new_user(id,location="USA",update_pref="daily",min_days_bewtween_msg=0,platform="twitter"): #for location by county & state, set location to "{County}, {State}".format(). ID is string.
        connection = db.create_connection(data.sql_addr, userbase.sql_user, userbase.sql_pass, data.db_name)
        fips = data.fips_from_text(location)
        db.insert(connection,userbase.table_from_param(platform),formatt=["id","location","update_pref","min_days_between_msg"],valueset=[id,fips,update_pref,min_days_bewtween_msg])
        db.commit(connection)
        connection.close()

        #    if userbase.log:
        #        print("[UPLOADERROR] error uploading, probable duplicate entry with id ",id) cant catch duplicate id?? bug

    def set(id,field,new_value,platform="twitter"): #set attribute of user
        connection = db.create_connection(data.sql_addr, userbase.sql_user, userbase.sql_pass, data.db_name)
        db.update(connection,userbase.table_from_param(platform),field,new_value,condition=f"id = '{id}'")
        db.commit(connection)
        connection.close()

    def get(id,platform="twitter"): #get dict of all user data
        connection = db.create_connection(data.sql_addr, userbase.sql_user, userbase.sql_pass, data.db_name)
        udata_raw = db.query(connection,userbase.table_from_param(platform),"*",condition=f"id = '{id}'")
        cnames = db.get_column_names(connection,userbase.table_from_param(platform))
        connection.close()
        if len(udata_raw)>0:
            udata = {}
            for i in range(0,len(cnames)):
                udata[cnames[i]] = udata_raw[0][i]
            return udata
        else:
            return None

    def if_user_exists(id,platform="twitter"):
        connection = db.create_connection(data.sql_addr, userbase.sql_user, userbase.sql_pass, data.db_name)
        r = db.query(connection,userbase.table_from_param(platform),"id",condition=f"id = '{id}'")
        connnection.close()
        if len(r) > 0:
            return True
        return False

    def remove_user(id,platform="twitter"):
        connection = db.create_connection(data.sql_addr, userbase.sql_user, userbase.sql_pass, data.db_name)
        db.delete(connection,userbase.table_from_param(platform),f"id = '{id}'")
        db.commit(connection)
        connection.close()

if __name__ == '__main__':
    userbase.log = True
    userbase.new_user('hank',location='Orange, California')
    print(userbase.get('hank'))
    userbase.set('hank','location',userbase.fips_from_text('Contra Costa, California'))
    print(userbase.get('hank'))
    userbase.remove_user('hank')
    print(userbase.get('hank'))
