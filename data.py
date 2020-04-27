import csv
import io
from urllib.request import urlopen
import datetime
import time
import pandas as pd
import censusdata
import matplotlib
import numpy as np
import matplotlib.pyplot as plt
from sql import db

ny_api = "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv"

#table names
db_name = "covidbot"
county_metadata = "META"
state_metadata = "state"

#specials
undivided_states = {"Puerto Rico":72,"Guam":66,"Virgin Islands":78,"Northern Mariana Islands":69,"American Samoa":60}
cities = {"Kansas City_Missouri":29998,"New York City_New York":36998}

#unknown counties have table 'ID'+state_fips+'999'


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

def plot_cases(County,State,filename="plt",show=False):
    if filename == "plt":
        filename += "-"+County+"-"+State+".png"
    connection = db.create_connection("localhost", "miles", "shrek5", db_name)
    FIPS = db.query(connection,county_metadata,"FIPS",["county = '{}'".format(County),"state = '{}'".format(State)])[0][0]
    table = "ID"+str(FIPS)
    dates = db.query(connection,table,"date")
    total_cases = db.query(connection,table,"total_cases")
    total_deaths = db.query(connection,table,"total_deaths")

    fig, ax = plt.subplots()
    ax.plot(dates,total_cases,color='blue',label='Cases')
    ax.plot(dates,total_deaths,color='red',label='Deaths')
    plt.legend()
    plt.xticks(rotation=25)
    ax.set(xlabel='Date Reported', ylabel='Num',title='COVID-19 Effect in Orange County')
    ax.grid()
    fig.savefig(filename)
    if show:
        plt.show()
    return True

if __name__ == "__main__":

    db.log = False
    plot_cases("Orange","California",show=True)

    #print(pull_from_nyt(update_past_date="2020-04-23",log=True,end_on_data_skip=True,highlight_errors=True,update_state=True)) #update_past_date="2020-04-20",
    #pull_from_census()
