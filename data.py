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

class data:
    ny_api = "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv"

    #sql login
    sql_addr = "localhost"
    sql_user = "miles"
    sql_pass = "shrek5"

    #table names
    db_name = "covidbot"
    county_metadata = "META"
    state_metadata = "state"

    #specials
    undivided_states = {"Puerto Rico":72,"Guam":66,"Virgin Islands":78,"Northern Mariana Islands":69,"American Samoa":60}
    cities = {"Kansas City_Missouri":29998,"New York City_New York":36998}

    #unknown counties have table 'ID'+state_fips+'999'

    def to_datetime(d): #d = string YYYY-MM-DD
        year = d[:4]
        month = d[5:7]
        day = d[8:10]
        dt = datetime.date(int(year),int(month),int(day))
        fdt = dt.strftime('%Y-%m-%d')
        return fdt
        #print(year," ",month," ",day)

    def if_string_is_int(s):
        try:
            int(s)
            return True
        except ValueError:
            return False

    def fips_from_text(location):
        if data.if_string_is_int(location):
            return int(location)
        connection = db.create_connection(data.sql_addr, data.sql_user, data.sql_pass, data.db_name)
        if ", " in location:
            county,state = location.split(", ")
            FIPS = db.query(connection,data.county_metadata,"FIPS",["county = '{}'".format(county),"state = '{}'".format(state)])[0][0]
            connection.close()
            return FIPS
        res = db.query(connection,data.state_metadata,"code",["state = '{}'".format(location)])
        if len(res)>0:
            connection.close()
            return res[0][0]
        connection.close()
        return 0

    def text_from_fips(fips):
        fips = str(fips)
        connection = db.create_connection(data.sql_addr, data.sql_user, data.sql_pass, data.db_name)
        res = None
        outstr = None
        if len(fips) > 2:
            res = db.query(connection,data.county_metadata,"county, state","FIPS = {}".format(fips))
            if len(res) > 0:
                outstr = "{}, {}".format(res[0][0],res[0][1])
        elif fips != "0":
            res = db.query(connection,data.state_metadata,"state","code = {}".format(fips))
            if len(res) > 0:
                outstr = res[0][0]
        else:
            outstr = "USA"
        if outstr != None:
            return outstr
        else:
            return None

    def c_s_from_text(location):
        if ", " in location:
            county,state = location.split(", ")
            return (county, state)
        else:
            return ("All Counties", location)

    def update_covid_data(retries=5,commit_every_insert=False,log=False,end_on_data_skip=False,highlight_errors=False,update_past_date="2020-01-20",update_state=False):
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
                connection = db.create_connection(data.sql_addr, data.sql_user, data.sql_pass, data.db_name)

                if connection != None:
                    logtab = ""
                    if highlight_errors:
                        logtab="\t"
                    current_tries = 0

                    response = urlopen(data.ny_api)
                    cr = csv.reader(line.decode() for line in response)
                    for row in cr:
                        if row[1]!="county" and data.to_datetime(row[0])>=data.to_datetime(last_committed_date):
                            last_updated_date = row[0]
                            if data.to_datetime(prev_date) < data.to_datetime(row[0]):
                                prev_date = row[0]
                                db.commit(connection)
                                last_committed_date = row[0]
                            if log:
                                print("\t",row)
                            if update_state and row[3] != '':
                                c = db.query(connection,data.state_metadata,fieldset="state",condition="state = '"+row[2]+"'")
                                if len(c) == 0:
                                    if log:
                                        print(logtab,"[NEWDATA] new state detected, adding to table")
                                    state_fips = row[3][:2]

                                    db.insert(connection,data.state_metadata,[row[2],int(state_fips)],["state","code"])
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
                                c = db.query(connection,data.county_metadata,fieldset="FIPS",condition=["state = '"+row[2]+"'","county = '"+row[1]+"'"])
                                if len(c) != 0:
                                    row[3] = str(c[0][0])
                                    if log:
                                        print(logtab,"[ERRORCATCH] empty FIPS field discovered, FIPS was found.")
                                elif row[1] == 'Unknown':
                                    cc = db.query(connection,data.state_metadata,fieldset="code",condition="state = '"+row[2]+"'")
                                    if len(cc) == 0:
                                        if row[2] in data.undivided_states:
                                            row[3] = str(data.undivided_states[row[2]])+"999"
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
                                elif row[1]+"_"+row[2] in data.cities:
                                    row[3] = str(data.cities[row[1]+"_"+row[2]])
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
                            c = db.query(connection,data.county_metadata,fieldset="FIPS",condition="FIPS = "+row[3])
                            if len(c) == 0:
                                db.insert(connection,data.county_metadata,[int(row[3]),row[1],row[2]],["FIPS","county","state"])
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
                                if len(db.query(connection, current_table, condition="date = '"+data.to_datetime(row[0])+"'")) == 0:
                                    do_insert = True
                            if do_insert:
                                db.insert(connection,current_table,[data.to_datetime(row[0]),int(row[4]),int(row[5])],formatt=["date","total_cases","total_deaths"])
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

    def update_population_data(log=False):
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

        connection = db.create_connection(data.sql_addr, data.sql_user, data.sql_pass, data.db_name)

        for index, row in county65plus.iterrows():
            #l = columnData.tolist()
            #print('row: ', index,' | data: ', row)
            fips = int(""+index.params()[0][1]+index.params()[1][1])
            #print(str(fips))

            c = db.query(connection,data.county_metadata,fieldset=["population","percent_65plus"],condition="FIPS = "+str(fips))
            if len(c) == 0:
                continue
            elif len(c) > 1:
                raise Warning(data.county_metadata+" database contains duplicate entries!")
                exit()
            if(type(c[0][0]==None)):
                if(log):
                    print("data: ",row["population_size"],",",row["percent_65plus"])
                db.update_multiple(connection,data.county_metadata,["population","percent_65plus"],[row["population_size"],row["percent_65plus"]],"FIPS = {}".format(fips))

        db.commit(connection)
        connection.close()

    def get_county_data(fips, date="*"): #{date: (caseNum, deathNum) }. fun fact: if data exists for county on certain day, it is still uploaded to api, even if data is the same as the prior day's data. as such there are no skipped days. keep in mind when calcing new cases
        connection = db.create_connection(data.sql_addr, data.sql_user, data.sql_pass, data.db_name)
        table = "ID"+str(fips)
        if date == "*":
            dataraw = db.query(connection,table,"*")
        else:
            dataraw = db.query(connection,table,"*",condition="date = '{}'".format(date))
        dataclean = {}
        for dr in dataraw:
            dataclean[str(dr[0])] = (dr[1],dr[2])
        return dataclean

    def plot(fips, filename="default",show=False,scale="linear"):
        text = data.text_from_fips(fips)
        if filename == "default":
            County, State = data.c_s_from_text(text)
            filename = "plot-"+County+"-"+State+".png"
        table = "ID"+str(fips)
        connection = db.create_connection(data.sql_addr, data.sql_user, data.sql_pass, data.db_name)
        dates = db.query(connection,table,"date")
        total_cases = db.query(connection,table,"total_cases")
        total_deaths = db.query(connection,table,"total_deaths")

        fig, ax = plt.subplots()
        ax.plot(dates,total_cases,color='blue',label='Cases')
        ax.plot(dates,total_deaths,color='red',label='Deaths')
        plt.legend()
        plt.xticks(rotation=25)
        plt.yscale(scale)
        ax.set(xlabel='Date Reported', ylabel='Num',title='COVID-19 Effect in {}'.format(text))
        ax.grid()
        fig.savefig(filename)
        if show:
            plt.show()
        connection.close()
        return True

if __name__ == "__main__":

    db.log = False
    connection = db.create_connection(data.sql_addr, data.sql_user, data.sql_pass, data.db_name)
    fips = data.fips_from_text("Contra Costa, California")
    data.plot(fips,show=True,scale="log")

    #print(data.update_covid_data(update_past_date="2020-04-28",log=True,end_on_data_skip=True,highlight_errors=True,update_state=True)) #update_past_date="2020-04-20",
    #data.update_population_data()
