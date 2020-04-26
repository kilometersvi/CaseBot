import csv
import io
from urllib.request import urlopen
import mysql.connector
from mysql.connector import Error
from mysql.connector import errors
import datetime
import time
import pandas as pd
import censusdata
import matplotlib
import numpy as np
import matplotlib.pyplot as plt

ny_api = "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv"

#table names
db_name = "covidbot"
county_metadata = "META"
state_metadata = "state"

#specials
undivided_states = {"Puerto Rico":72,"Guam":66,"Virgin Islands":78,"Northern Mariana Islands":69,"American Samoa":60}
cities = {"Kansas City_Missouri":29998,"New York City_New York":36998}

#unknown counties have table 'ID'+state_fips+'999'
class db:
    log = False
    def create_connection(host_name, user_name, user_password, db_name='mysql'):
        connection = None
        try:
            connection = mysql.connector.connect(
                host=host_name,
                user=user_name,
                passwd=user_password,
                database=db_name
            )
            if db.log:
                print("\t[SUCCESS] Connected to DB.")
        except Error as e:
            if db.log:
                print(f"\t[FAILURE] The error '{e}' occurred while connecting to DB.")
        return connection

    def where_gen(condition):
        where = ""
        if condition != None:
            if isinstance(condition,str):
                if condition[:5] != "WHERE":
                    where += "WHERE "+condition+" "
                else:
                    where += condition+" "
            if isinstance(condition,list):
                where += "WHERE "
                for i in range(0,len(condition)):
                    p = condition[i]
                    if i != 0 and not (p[:3] == "AND" or p[:2] == "OR"):
                        where += " AND "
                    where += p+" "
        #print(where)
        return where

    def run(connection, cmd):
        cursor = connection.cursor(buffered=True)
        #cursor.close()
        #cursor = connection.cursor()
        try:
            cursor.execute(cmd)
            outstring = "\t[SUCCESS] Database exec'd successfully: {}".format(cmd)

            results = []
            #numResults = 0
            errorResult = False
            try:
                results = cursor.fetchall()
                #outstring += " ({} RETURNED) ".format(str(len(cursor.fetchall())))
            except Error as ie:
                errorResult = True
                #print("caught error: "+str(ie))
                pass
            #print(numResults)
            #if numResults == None:
            #    return 0
            #else:
            #    return numResults

            if(not errorResult):
                outstring += " ({} returned)".format(str(len(results)))

            if db.log:
                print(outstring)

            return results#cursor

        except Error as e:
            raise Warning("[EXECERROR]\n\tCommand: {}\n\tError: {}".format(cmd,str(e)))
            if db.log:
                print(f"[EXECERROR] The error '{e}' occurred")

    def insert(connection, table, valueset, formatt=None): #if format ignored, valueset must be dict
        # valueset = list or dict, formatt = list
        format_str = "("
        values_str = "VALUES ("
        if formatt == None:
            if isinstance(valueset,dict):
                formatt = valueset.keys()
            else:
                raise SyntaxError("[IMPLEMENTERROR] invalid insertion data")
        count = 0
        string_valueset = None
        if isinstance(valueset,list):
            string_valueset = []
            for v in valueset:
                string_valueset.append(str(v))
        elif isinstance(valueset,dict):
            string_valueset = {}
            for key in valueset:
                string_valueset[key] = str(valueset[key])
        for i in range(0,len(formatt)):
            f = formatt[i]
            format_str += f+", "
            dobrack = ""
            if isinstance(valueset[i],str):
                dobrack = "'"
            if isinstance(valueset,list):
                values_str += dobrack+"{}"+dobrack+", "
            elif isinstance(valueset,dict):
                values_str = dobrack+"{"+f+"}"+dobrack+", "

        format_str = format_str[:-2]+")"
        values_str = values_str[:-2]+")"

        cmd = "INSERT INTO "+table+" "+format_str+" "+values_str
        #print(cmd.format(*valueset))
        db.run(connection, cmd.format(*valueset))

    def update_multiple(connection, table, fieldset, valueset, condition=None):
        if(len(valueset) != len(fieldset)):
            raise SyntaxError("[IMPLEMENTERROR] incongruent fieldset & valueset")
        else:
            for i in range(0,len(fieldset)):
                db.update(connection,table,fieldset[i],valueset[i],condition)

    def update(connection, table, field, value, condition=None):
        dobrack = ""
        if isinstance(value,str):
            dobrack = "'"
        cmd = "UPDATE "+table+" SET "+field+" = "+dobrack+str(value)+dobrack+" "+db.where_gen(condition)
        db.run(connection, cmd)

    def query(connection, table, fieldset="*", condition=None):
        query_str = "SELECT "
        if isinstance(fieldset, str):
            query_str += fieldset
        else:
            for f in fieldset:
                query_str += f+", "
            query_str = query_str[:-2]+" "
        query_str += " FROM "+table+" "+db.where_gen(condition)
        return db.run(connection, query_str)

    def delete(connection, table, condition=None): #delete all records matching condition
        cmd = "DELETE FROM "+table+" "+db.where_gen(condition)
        db.run(connection, cmd)

    def if_table_exists(connection, table):
        c = db.run(connection,"show tables")
        #db.print_all(c)
        for (t) in c:
            #print(t[0]," compared to ",table)
            if table==t[0]:
                return True
        return False

    def commit(connection):
        try:
            connection.commit()
            if db.log:
                print("\t[SUCCESS] Database committed successfully")
            time.sleep(0.1)
            return True
        except Error as e:
            if db.log:
                print(f"[FAILURE] The error '{e}' occurredwhile committing.")
            raise Warning("[FAILURE] CommitError: "+e)
            return False

    def print_all(c):
        wasPrinted = False
        for row in c:
            wasPrinted = True
            print(row)
        if not wasPrinted:
            print("empty set")

def to_datetime(d):
    year = d[:4]
    month = d[5:7]
    day = d[8:10]
    dt = datetime.date(int(year),int(month),int(day))
    fdt = dt.strftime('%Y-%m-%d')
    return fdt
    #print(year," ",month," ",day)

def pull_from_nyt(retries=5,commit_every_insert=False,log=False,end_on_data_skip=False,highlight_errors=False,update_past_date="2020-01-20",update_state=False):
    '''
    0    1       2       3   4       5
    date,county,state,fips,cases,deaths
    '''
    last_updated_date = update_past_date
    was_data_skipped = False
    last_committed_date = update_past_date
    current_tries = 0
    finish = False
    prev_date = "2020-01-20"
    connection = None

    while(current_tries<retries and not finish):
        try:
            connection = db.create_connection("localhost", "miles", "shrek5", db_name)

            if connection != None:
                logtab = ""
                if highlight_errors:
                    logtab="\t"
                current_tries = 0

                response = urlopen(ny_api)
                cr = csv.reader(line.decode() for line in response)
                for row in cr:
                    if row[1]!="county" and to_datetime(row[0])>=to_datetime(last_committed_date):
                        last_updated_date = row[0]
                        if to_datetime(prev_date) < to_datetime(row[0]):
                            prev_date = row[0]
                            db.commit(connection)
                            last_committed_date = row[0]
                        if log:
                            print("\t",row)
                        if update_state and row[3] != '':
                            c = db.query(connection,state_metadata,fieldset="state",condition="state = '"+row[2]+"'")
                            if len(c) == 0:
                                if log:
                                    print(logtab,"[NEWDATA] new state detected, adding to table")
                                state_fips = row[3][:2]

                                db.insert(connection,state_metadata,[row[2],int(state_fips)],["state","code"])
                                db.commit(connection)
                                last_committed_date = row[0]

                        for i in range(1,3):
                            if "'" in row[i]:
                                if log:
                                    print(logtab,"[ERRORCATCH] detected invalid char in field, avoided")
                                #print(row[i])
                                row[i] = row[i].replace("'","\\'")
                                #print(row[i])
                        if row[3] == '':
                            c = db.query(connection,county_metadata,fieldset="FIPS",condition=["state = '"+row[2]+"'","county = '"+row[1]+"'"])
                            if len(c) != 0:
                                row[3] = str(c[0][0])
                                if log:
                                    print(logtab,"[ERRORCATCH] empty FIPS field discovered, FIPS was found.")
                            elif row[1] == 'Unknown':
                                cc = db.query(connection,state_metadata,fieldset="code",condition="state = '"+row[2]+"'")
                                if len(cc) == 0:
                                    if row[2] in undivided_states:
                                        row[3] = str(undivided_states[row[2]])+"999"
                                        if log:
                                            print(logtab,"[ERRORCATCH] uncountied state detected.")
                                    else:
                                        if log:
                                            print("[ERRORCATCH_FAILURE] 'Unknown' county detected, but state is unknown. data skipped")
                                        was_data_skipped = True
                                        if end_on_data_skip:
                                            exit()
                                        continue
                                else:
                                    row[3] = ""+str(cc[0][0])+"999"
                                    if log:
                                        print(logtab,"[ERRORCATCH] adjusted for Unknown county")
                            elif row[1]+"_"+row[2] in cities:
                                row[3] = str(cities[row[1]+"_"+row[2]])
                                if log:
                                    print(logtab,"[ERRORCATCH] adjusted for city, county changed to ",row[3])
                            else:
                                if log:
                                    print("[ERRORCATCH_FAILURE] could not adjust from empty FIPS, data skipped")
                                was_data_skipped = True
                                if end_on_data_skip:
                                    exit()
                                continue
                        #print('\tcounty: ',row[3])
                        c = db.query(connection,county_metadata,fieldset="FIPS",condition="FIPS = "+row[3])
                        if len(c) == 0:
                            db.insert(connection,county_metadata,[int(row[3]),row[1],row[2]],["FIPS","county","state"])
                        elif len(c) > 1:
                            raise Warning("META database contains duplicate entries!")
                            exit()
                        #else:
                        #    (r_FIPS,r_county,r_state,r_pop) = c.fetchall()[0]
                        current_table = "ID"+str(int(row[3]))
                        do_insert = False
                        if not db.if_table_exists(connection,current_table):
                            if log:
                                print(logtab,"[NEWDATA] new county detected, creating table")
                            db.run(connection, "create table {} (date DATE, total_cases INT, total_deaths INT)".format(current_table))
                            db.commit(connection)
                            last_committed_date = row[0]
                            do_insert = True
                        else:
                            if len(db.query(connection, current_table, condition="date = '"+to_datetime(row[0])+"'")) == 0:
                                do_insert = True
                        if do_insert:
                            db.insert(connection,current_table,[to_datetime(row[0]),int(row[4]),int(row[5])],formatt=["date","total_cases","total_deaths"])
                            if(commit_every_insert):
                                db.commit(connection)
                                last_committed_date = row[0]
                        #print("looped")
                finish = True
            else:
                if log:
                    print(logtab+"[ConnectionError] retrying...")
                time.sleep(5)
                current_tries+=1
        except Exception as eee:
            if log:
                print(logtab+str(eee)+", retrying...")
            time.sleep(5)
            current_tries+=1

    if finish:
        db.commit(connection)
        last_committed_date = last_updated_date
        connection.close()
    else:
        raise Warning("Connection to SQL Server lost.")
    return last_committed_date

def get_population_data(log=False):
    pd.set_option('display.expand_frame_repr', False)
    pd.set_option('display.precision', 2)

    county65plus = censusdata.download('acs5', 2015, censusdata.censusgeo([('county', '*')]),
                                       ['B01001_001E', 'B01001_020E', 'B01001_021E', 'B01001_022E', 'B01001_023E',
                                        'B01001_024E', 'B01001_025E', 'B01001_044E', 'B01001_045E', 'B01001_046E',
                                        'B01001_047E', 'B01001_048E', 'B01001_049E'])
    #county65plus.describe()
    county65plus['percent_65plus'] = (county65plus.B01001_020E + county65plus.B01001_021E + county65plus.B01001_022E
                                  + county65plus.B01001_023E + county65plus.B01001_024E + county65plus.B01001_025E
                                  + county65plus.B01001_044E + county65plus.B01001_045E + county65plus.B01001_046E
                                  + county65plus.B01001_047E + county65plus.B01001_048E
                                  + county65plus.B01001_049E) / county65plus.B01001_001E * 100
    county65plus = county65plus[['B01001_001E', 'percent_65plus']]
    county65plus = county65plus.rename(columns={'B01001_001E': 'population_size'})
    #county65plus.describe()
    #county65plus.sort_values('percent_65plus', ascending=False, inplace=True)
    #county65plus.head(30)

    connection = db.create_connection("localhost", "miles", "shrek5", db_name)

    for index, row in county65plus.iterrows():
        #l = columnData.tolist()
        #print('row: ', index,' | data: ', row)
        fips = int(""+index.params()[0][1]+index.params()[1][1])
        #print(str(fips))

        c = db.query(connection,county_metadata,fieldset=["population","percent_65plus"],condition="FIPS = "+str(fips))
        if len(c) == 0:
            continue
        elif len(c) > 1:
            raise Warning(county_metadata+" database contains duplicate entries!")
            exit()
        if(type(c[0][0]==None)):
            if(log):
                print("data: ",row["population_size"],",",row["percent_65plus"])
            db.update_multiple(connection,county_metadata,["population","percent_65plus"],[row["population_size"],row["percent_65plus"]],"FIPS = {}".format(fips))

    db.commit(connection)
    connection.close()

def plot_cases(County,State):
    connection = db.create_connection("localhost", "miles", "shrek5", db_name)
    FIPS = db.query(connection,county_metadata,"FIPS",["county = '{}'".format(County),"state = '{}'".format(State)])[0][0]
    table = "ID"+str(FIPS)
    dates =


if __name__ == "__main__":

    db.log = False
    plot_cases("Contra Costa","California")

    #print(pull_from_nyt(update_past_date="2020-04-23",log=True,end_on_data_skip=True,highlight_errors=True,update_state=True)) #update_past_date="2020-04-20",
    #pull_from_census()
