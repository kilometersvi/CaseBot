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
import sys, traceback
from func_timeout import func_timeout,FunctionTimedOut

class data:
    ny_county_api = "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv"
    ny_state_api = "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-states.csv"
    #sql login
    sql_addr = "localhost"
    sql_user = "miles"
    sql_pass = "shrek5"

    #table names
    db_name = "covidbot"
    county_metadata = "meta_county"
    state_metadata = "meta_state"
    misc_data = "meta_misc"

    #specials
    exception_states = {"Puerto Rico":72,"Guam":66,"Virgin Islands":78,"Northern Mariana Islands":69,"American Samoa":60,"Rhode Island":44}
    cities = {"Kansas City_Missouri":2938000,"New York City_New York":3651000}

    #unknown counties have table 'ID'+state_fips+'999'

    #init functions
    def init_db():
        connection = db.create_connection(data.sql_addr, data.sql_user, data.sql_pass)
        db.run(connection, "CREATE DATABASE IF NOT EXISTS covidbot")
        data.create_meta_tables(connection)
        connection.close()

    def create_meta_tables(connection,log=False):
        if not db.if_table_exists(connection, data.county_metadata):
            if log:
                print("[NEWDATA] county metadata table does not exist, creating")
            db.run(connection, "create table `{}` (FIPS INT, county VARCHAR(36), state VARCHAR(24), population INT, percent_0to9 FLOAT, percent_10to19 FLOAT, percent_20to29 FLOAT, percent_30to39 FLOAT, percent_40to49 FLOAT, percent_50to59 FLOAT, percent_60to69 FLOAT, percent_70to79 FLOAT, percent_80plus FLOAT)".format(data.county_metadata))
            db.commit(connection)

        if not db.if_table_exists(connection, data.state_metadata):
            if log:
                print("[NEWDATA] state metadata table does not exist, creating")
            db.run(connection, "create table `{}` (state VARCHAR(36), FIPS MEDIUMINT(9), population INT, percent_0to9 FLOAT, percent_10to19 FLOAT, percent_20to29 FLOAT, percent_30to39 FLOAT, percent_40to49 FLOAT, percent_50to59 FLOAT, percent_60to69 FLOAT, percent_70to79 FLOAT, percent_80plus FLOAT)".format(data.state_metadata))
            db.commit(connection)

        if not db.if_table_exists(connection, data.misc_data):
            if log:
                print("[NEWDATA] misc data table does not exist, creating")
            db.run(connection, "create table `{}` (i INT AUTO_INCREMENT NOT NULL, var VARCHAR(36) NOT NULL, value VARCHAR(64), primary key (i) )".format(data.misc_data))
            db.insert(connection,data.misc_data,["last_committed_date","2020-01-20"],["var", "value"])
            db.insert(connection,data.misc_data,["last_committed_state_date","2020-01-20"],["var", "value"])
            db.commit(connection)
            #last_committed_date = "2020-01-20"

    #helper functions
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
        print(location)
        if data.if_string_is_int(location):
            return int(location)
        connection = db.create_connection(data.sql_addr, data.sql_user, data.sql_pass, data.db_name)
        if ", " in location:
            county,state = location.split(", ")
            #county.replace(" county","")
            #county.replace(" County","")

            FIPS = db.query(connection,data.county_metadata,"FIPS",["county = '{}'".format(county),"state = '{}'".format(state)])[0][0]
            connection.close()
            return FIPS
        res = db.query(connection,data.state_metadata,"FIPS",["state = '{}'".format(location)])
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
            res = db.query(connection,data.state_metadata,"state","FIPS = {}".format(fips))
            if len(res) > 0:
                outstr = res[0][0]
        else:
            outstr = "USA"
        if outstr != None:
            return outstr
        else:
            return None

    def c_s_from_text(location): # -> (County, State)
        if ", " in location:
            county,state = location.split(", ")
            return (county, state)
        else:
            return ("All Counties", location)

    #update functions
    def update_all(retries=5,log=False,commit_every_insert=False,end_on_data_skip=False,highlight_errors=False,update_state_in_county_update=True,min_wait_time=20,min_deaths_to_county_pop_pull=50):
        data.init_db()

        was_new_county_found = data.update_county_data(retries=retries,log=log,commit_every_insert=commit_every_insert,end_on_data_skip=end_on_data_skip,highlight_errors=highlight_errors,update_state=update_state_in_county_update,min_wait_time=min_wait_time,min_deaths_to_county_pop_pull=min_deaths_to_county_pop_pull)

        if was_new_county_found:
            data.update_all_population_data(log=log,onlyTotal=True)

        data.update_state_data(retries=retries,log=log,commit_every_insert=commit_every_insert,highlight_errors=highlight_errors,min_wait_time=min_wait_time)

        if log:
            print("Data updated.")

    def update_county_data(retries=5,commit_every_insert=False,log=False,end_on_data_skip=False,highlight_errors=False,update_state=True,min_wait_time=20,min_deaths_to_county_pop_pull=50): # if mdtcpp = -1, dont pull county pop data

        last_updated_date = None
        was_data_skipped = False
        last_committed_date = None
        current_tries = 0
        finish = False
        prev_date = "2020-01-20"
        connection = None
        was_new_county_found = False

        while(current_tries<retries and not finish):
            try:
                connection = db.create_connection(data.sql_addr, data.sql_user, data.sql_pass, data.db_name)

                if connection != None:
                    logtab = ""
                    if highlight_errors:
                        logtab="\t"
                    current_tries = 0

                    data.create_meta_tables(connection,log)

                    last_committed_date = db.query(connection, data.misc_data, "value", "var = 'last_committed_date'")[0][0]
                    last_updated_date = last_committed_date

                    '''
                    0    1       2       3   4       5
                    date,county,state,fips,cases,deaths
                    '''
                    if log:
                        print(logtab,'[PULL] downloading NYT Covid19 county data...')
                    response = urlopen(data.ny_county_api)
                    cr = csv.reader(line.decode() for line in response)
                    for row in cr:
                        if row[1]!="county" and data.to_datetime(row[0])>=data.to_datetime(last_committed_date):
                            last_updated_date = row[0]
                            if data.to_datetime(prev_date) < data.to_datetime(row[0]):
                                prev_date = row[0]
                                last_committed_date = row[0]
                                db.update(connection, data.misc_data, "value", str(last_committed_date), condition="var = 'last_committed_date'")
                                db.commit(connection)
                            if log:
                                print("\t",row)
                            if update_state and row[3] != '':
                                c = db.query(connection,data.state_metadata,fieldset="state",condition="state = '{}'".format(row[2]))
                                if len(c) == 0:
                                    if log:
                                        print(logtab,'[NEWDATA] new state detected, adding to table')
                                    state_fips = row[3][:2]
                                    popdata = func_timeout(min_wait_time+10*current_tries,data.pull_population_data,args=[state_fips],kwargs={"log":log})
                                    if not popdata.empty:
                                        for index, rowc in popdata.iterrows():
                                            #fips = int(""+index.params()[0][1]+index.params()[1][1])
                                            db.insert(connection,data.state_metadata,
                                                  [row[2], int(state_fips), rowc["population_size"], rowc['percent_0to9'], rowc['percent_10to19'], rowc['percent_20to29'],
                                                   rowc['percent_30to39'] ,rowc['percent_40to49'], rowc['percent_50to59'], rowc['percent_60to69'],
                                                   rowc['percent_70to79'], rowc['percent_80plus']],
                                                  ["state","FIPS","population",'percent_0to9', 'percent_10to19', 'percent_20to29', 'percent_30to39',
                                                  'percent_40to49', 'percent_50to59', 'percent_60to69', 'percent_70to79', 'percent_80plus'])
                                            break
                                    else:
                                        db.insert(connection,data.state_metadata,[row[2], int(state_fips)],["state","FIPS"])

                                    last_committed_date = row[0]
                                    db.update(connection, data.misc_data, "value", str(last_committed_date), condition="var = 'last_committed_date'")
                                    db.commit(connection)
                            for i in range(1,3):
                                if "'" in row[i]:
                                    if log:
                                        print(logtab,'[ERRORCATCH] detected invalid char in field, adjusted')
                                    row[i] = row[i].replace("'","\\'")

                            is_real_fips = True
                            if row[3] == '':
                                c = db.query(connection,data.county_metadata,fieldset="FIPS",condition=["state = '"+row[2]+"'","county = '"+row[1]+"'"])
                                if len(c) != 0:
                                    row[3] = str(c[0][0])
                                    if log:
                                        print(logtab,'[ERRORCATCH] empty FIPS field discovered, FIPS found as ',row[3])
                                elif row[1] == 'Unknown':
                                    is_real_fips = False
                                    cc = db.query(connection,data.state_metadata,fieldset="FIPS",condition="state = '"+row[2]+"'")
                                    if len(cc) == 0:
                                        if row[2] in data.exception_states:
                                            row[3] = str(data.exception_states[row[2]])+"999"
                                            if log:
                                                print(logtab,'[ERRORCATCH] encountered either uncountied state or "unknown" county in previously undetected state. FIPS found as '+row[3])
                                        else:
                                            if log:
                                                print("[ERRORCATCH_FAILURE] 'Unknown' county detected, but state is unknown. data skipped")
                                            was_data_skipped = True
                                            if end_on_data_skip:
                                                exit()
                                            continue
                                    else:
                                        is_real_fips = False
                                        row[3] = ""+str(cc[0][0])+"999"
                                        if log:
                                            print(logtab,'[ERRORCATCH] adjusted for Unknown county')
                                elif row[1]+"_"+row[2] in data.cities:
                                    #is_real_fips = False
                                    row[3] = str(data.cities[row[1]+"_"+row[2]])
                                    if log:
                                        print(logtab,'[ERRORCATCH] adjusted for city, county changed to ',row[3])
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
                                was_new_county_found = True
                                if log:
                                    print(logtab,f'[NEWDATA] new county {row[3]} detected, adding to table (real FIPS code: {is_real_fips})')
                                db.insert(connection,data.county_metadata,[int(row[3]),row[1],row[2]],["FIPS","county","state"])

                                '''popdata = func_timeout(min_wait_time+10*current_tries,data.pull_population_data,args=[int(row[3])],kwargs={"log":log})
                                for index, rowc in popdata.iterrows():
                                    #fips = int(""+index.params()[0][1]+index.params()[1][1])
                                    db.insert(connection,data.county_metadata,
                                              [int(row[3]),row[1],row[2],rowc["population_size"], rowc['percent_0to9'], rowc['percent_10to19'], rowc['percent_20to29'],
                                               rowc['percent_30to39'] ,rowc['percent_40to49'], rowc['percent_50to59'], rowc['percent_60to69'],
                                               rowc['percent_70to79'], rowc['percent_80plus']],
                                              ["FIPS","county","state","population",'percent_0to9', 'percent_10to19', 'percent_20to29', 'percent_30to39',
                                              'percent_40to49', 'percent_50to59', 'percent_60to69', 'percent_70to79', 'percent_80plus'])
                                    break'''

                                if commit_every_insert:
                                    last_committed_date = row[0]
                                    db.update(connection, data.misc_data, "value", str(last_committed_date), condition="var = 'last_committed_date'")
                                    db.commit(connection)
                            elif len(c) > 1:
                                raise Warning("META database contains duplicate entries!")
                                exit()

                            current_table = "ID"+str(int(row[3]))
                            do_insert = False
                            if not db.if_table_exists(connection,current_table):
                                if log:
                                    print(logtab,'[NEWDATA] new county detected, creating table')
                                db.run(connection, "create table {} (date DATE, total_cases INT, total_deaths INT)".format(current_table))
                                last_committed_date = row[0]
                                db.update(connection, data.misc_data, "value", str(last_committed_date), condition="var = 'last_committed_date'")
                                db.commit(connection)
                                do_insert = True
                            else:
                                if len(db.query(connection, current_table, condition="date = '{}'".format(data.to_datetime(row[0])))) == 0:
                                    do_insert = True
                            if is_real_fips and min_deaths_to_county_pop_pull <= int(row[5]):
                                ccc = db.query(connection,data.county_metadata,fieldset="percent_0to9",condition="FIPS = {}".format(int(row[3])))
                                if len(ccc) == 0 or ccc[0][0] == None:
                                    popdata = func_timeout(min_wait_time+10*current_tries,data.pull_population_data,args=[int(row[3])],kwargs={"log":log})
                                    if not popdata.empty:
                                        for index, rowc in popdata.iterrows():
                                            db.update(connection,data.county_metadata,fieldset=
                                                      ["population",'percent_0to9', 'percent_10to19', 'percent_20to29', 'percent_30to39',
                                                      'percent_40to49', 'percent_50to59', 'percent_60to69', 'percent_70to79', 'percent_80plus'],
                                                      valueset=[rowc["population_size"], rowc['percent_0to9'], rowc['percent_10to19'], rowc['percent_20to29'],
                                                      rowc['percent_30to39'] ,rowc['percent_40to49'], rowc['percent_50to59'], rowc['percent_60to69'],
                                                      rowc['percent_70to79'], rowc['percent_80plus']],condition="FIPS = {}".format(row[3]))
                                    elif log:
                                        print(logtab,"[POPDATA] dont need to fetch pop data, already updated")
                            if do_insert:
                                db.insert(connection,current_table,[data.to_datetime(row[0]),int(row[4]),int(row[5])],formatt=["date","total_cases","total_deaths"])
                                if commit_every_insert:
                                    last_committed_date = row[0]
                                    db.update(connection, data.misc_data, "value", str(last_committed_date), condition="var = 'last_committed_date'")
                                    db.commit(connection)
                            #print("looped")
                    finish = True
                else:
                    if log:
                        print(logtab+"[ConnectionError] retrying...")
                    time.sleep(5)
                    current_tries+=1
            except Exception as eee:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                if log:
                    print(logtab,str(eee))
                    traceback.print_tb(exc_traceback, limit=1, file=sys.stdout)
                    print("retrying...")
                time.sleep(5)
                current_tries+=1
            except FunctionTimedOut as fto:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                if log:
                    print(logtab,str(fto))
                    traceback.print_tb(exc_traceback, limit=1, file=sys.stdout)
                    print("retrying...")
                time.sleep(5)
                current_tries+=1

        if finish:
            last_committed_date = last_updated_date
            db.update(connection, data.misc_data, "value", str(last_committed_date), condition="var = 'last_committed_date'")
            db.commit(connection)
            connection.close()
            if log:
                print("finished.")
        else:
            raise Warning("Connection to SQL Server lost.")

        return was_new_county_found

    def pull_population_data(fips,log=False,onlyTotal=False):
        params = []
        if fips != "*":
            fips = str(fips)

            pd.set_option('display.expand_frame_repr', False)
            pd.set_option('display.precision', 2)

            params = []
            if fips == '0': #USA
                print('unimplemented')
                quit()
            elif len(fips) >= 1 and len(fips) < 3: #state fips
                if len(fips) < 2:
                    fips = "0"+fips
                if log:
                    print(f'\t[POPDATA] {fips} detected as state')
                params = [('state',fips)]
            elif len(fips) > 5: #city fips
                place = fips[2:]
                state = fips[:2]
                if log:
                    print(f'\t[POPDATA] {fips} detected as place, state set to {state} and place to {place}')
                params = [('state',state),('place',place)]
            else: #county fips
                if len(fips) < 5:
                    fips = "0"+fips
                county = fips[2:]
                state = fips[:2]
                if county == '999':
                    if log:
                        print('\t[POPDATA_ERRORCATCH] cant get pop data for unknown')
                        return pd.DataFrame(columns = [""])
                if log:
                    print(f'\t[POPDATA] {fips} detected as county, state set to {state} and county to {county}')

                params = [('state',state),('county',county)]
        else:
            params = [('county', '*')]
            if log:
                print('\t[POPDATA] pulling all county data')
        if log:
            print('\t[PULL] pulling US Census Data...')

        if not onlyTotal:
            popdata = censusdata.download('acs5',2015,censusdata.censusgeo(params),
                                          ['B01001_001E','B01001_003E','B01001_004E','B01001_005E','B01001_006E','B01001_007E',
                                           'B01001_008E','B01001_009E','B01001_010E','B01001_011E','B01001_012E','B01001_013E',
                                           'B01001_014E','B01001_015E','B01001_016E','B01001_017E','B01001_018E','B01001_019E',
                                           'B01001_020E','B01001_021E','B01001_022E','B01001_023E','B01001_024E','B01001_025E',
                                           'B01001_027E','B01001_028E','B01001_029E','B01001_030E','B01001_031E','B01001_032E',
                                           'B01001_033E','B01001_034E','B01001_035E','B01001_036E','B01001_037E','B01001_038E',
                                           'B01001_039E','B01001_040E','B01001_041E','B01001_042E','B01001_043E','B01001_044E',
                                           'B01001_045E','B01001_046E','B01001_047E','B01001_048E','B01001_049E'])
            popdata['percent_0to9'] = (popdata.B01001_003E + popdata.B01001_004E + popdata.B01001_027E + popdata.B01001_028E) / popdata.B01001_001E * 100
            popdata['percent_10to19'] = (popdata.B01001_005E + popdata.B01001_006E + popdata.B01001_007E + popdata.B01001_029E + popdata.B01001_030E + popdata.B01001_031E) / popdata.B01001_001E * 100
            popdata['percent_20to29'] = (popdata.B01001_008E + popdata.B01001_009E + popdata.B01001_010E + popdata.B01001_011E + popdata.B01001_032E + popdata.B01001_033E + popdata.B01001_034E + popdata.B01001_035E) / popdata.B01001_001E * 100
            popdata['percent_30to39'] = (popdata.B01001_012E + popdata.B01001_013E + popdata.B01001_036E + popdata.B01001_037E) / popdata.B01001_001E * 100
            popdata['percent_40to49'] = (popdata.B01001_014E + popdata.B01001_015E + popdata.B01001_038E + popdata.B01001_039E) / popdata.B01001_001E * 100
            popdata['percent_50to59'] = (popdata.B01001_016E + popdata.B01001_017E + popdata.B01001_040E + popdata.B01001_041E) / popdata.B01001_001E * 100
            popdata['percent_60to69'] = (popdata.B01001_018E + popdata.B01001_019E + popdata.B01001_020E + popdata.B01001_021E + popdata.B01001_042E + popdata.B01001_043E + popdata.B01001_044E + popdata.B01001_045E) / popdata.B01001_001E * 100
            popdata['percent_70to79'] = (popdata.B01001_022E + popdata.B01001_023E + popdata.B01001_046E + popdata.B01001_047E) / popdata.B01001_001E * 100
            popdata['percent_80plus'] = (popdata.B01001_024E + popdata.B01001_025E + popdata.B01001_048E + popdata.B01001_049E) / popdata.B01001_001E * 100

            popdata = popdata[['B01001_001E', 'percent_0to9', 'percent_10to19', 'percent_20to29', 'percent_30to39',
                               'percent_40to49', 'percent_50to59', 'percent_60to69', 'percent_70to79', 'percent_80plus']]
        else:
            popdata = censusdata.download('acs5',2015,censusdata.censusgeo(params),['B01001_001E'])

        popdata = popdata.rename(columns={'B01001_001E': 'population_size'})

        return popdata

    def update_all_population_data(log=False,onlyTotal=False): #does not find city data

        '''
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
        '''
        popdata = data.pull_population_data('*',log=log,onlyTotal=onlyTotal)

        connection = db.create_connection(data.sql_addr, data.sql_user, data.sql_pass, data.db_name)

        for index, row in popdata.iterrows():
            #l = columnData.tolist()
            #print('row: ', index,' | data: ', row)
            fips = int(""+index.params()[0][1]+index.params()[1][1])

            c = db.query(connection,data.county_metadata,fieldset=["population"],condition="FIPS = "+str(fips))
            if len(c) == 0:
                continue
            elif len(c) > 1:
                raise Warning(data.county_metadata+" database contains duplicate entries!")
                exit()
            if(type(c[0][0]==None)):
                if log:
                    print("data: ",row)
                if not onlyTotal:
                    db.update(connection,data.county_metadata,
                                       ["population",'percent_0to9', 'percent_10to19', 'percent_20to29', 'percent_30to39',
                                       'percent_40to49', 'percent_50to59', 'percent_60to69', 'percent_70to79', 'percent_80plus'],
                                       [row["population_size"], row['percent_0to9'], row['percent_10to19'], row['percent_20to29'],
                                        row['percent_30to39'] ,row['percent_40to49'], row['percent_50to59'], row['percent_60to69'],
                                        row['percent_70to79'], row['percent_80plus']],
                                        "FIPS = {}".format(fips))
                else:
                    db.update(connection,data.county_metadata,"population",row["population_size"],"FIPS = {}".format(fips))

        db.commit(connection)
        connection.close()

    def update_state_data(log=False,retries=5,commit_every_insert=False,highlight_errors=False,min_wait_time=20):

        last_committed_date = None
        last_updated_date = None
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

                    data.create_meta_tables(connection,log)

                    last_committed_date = db.query(connection, data.misc_data, "value", "var = 'last_committed_state_date'")[0][0]
                    last_updated_date = last_committed_date

                    if log:
                        print(logtab,'[PULL] downloading NYT Covid19 state data...')
                    response = urlopen(data.ny_state_api)
                    cr = csv.reader(line.decode() for line in response)
                    '''
                    0    1     2    3     4
                    date,state,fips,cases,deaths
                    '''
                    for row in cr:
                        if row[1]!="state" and data.to_datetime(row[0])>=data.to_datetime(last_committed_date):
                            last_updated_date = row[0]
                            if data.to_datetime(prev_date) < data.to_datetime(row[0]):
                                prev_date = row[0]
                                last_committed_date = row[0]
                                db.update(connection, data.misc_data, "value", str(last_committed_date), condition="var = 'last_committed_state_date'")
                                db.commit(connection)
                            if log:
                                print("\t",row)

                            if "'" in row[1]:
                                if log:
                                    print(logtab,'[ERRORCATCH] detected invalid char in field, adjusted')
                                row[i] = row[i].replace("'","\\'")

                            c = db.query(connection,data.state_metadata,fieldset="FIPS",condition="FIPS = "+row[2])
                            if (len(c) == 0 or c[0][0] == None) and (int(row[2]) <= 56):
                                if log:
                                    print(logtab,f'[NEWDATA] new state {row[2]} detected, adding to table')

                                popdata = func_timeout(min_wait_time+10*current_tries,data.pull_population_data,args=[int(row[2])],kwargs={"log":log})
                                if not popdata.empty:
                                    for index, rowc in popdata.iterrows():
                                        #fips = int(""+index.params()[0][1]+index.params()[1][1])
                                        db.insert(connection,data.state_metadata,
                                                  [row[1],int(row[2]),rowc["population_size"], rowc['percent_0to9'], rowc['percent_10to19'], rowc['percent_20to29'],
                                                   rowc['percent_30to39'] ,rowc['percent_40to49'], rowc['percent_50to59'], rowc['percent_60to69'],
                                                   rowc['percent_70to79'], rowc['percent_80plus']],
                                                  ["state","FIPS","population",'percent_0to9', 'percent_10to19', 'percent_20to29', 'percent_30to39',
                                                  'percent_40to49', 'percent_50to59', 'percent_60to69', 'percent_70to79', 'percent_80plus'])
                                        break
                                else:
                                    db.insert(connection,data.state_metadata,[row[1],int(row[2])],["state","FIPS"])

                                if commit_every_insert:
                                    last_committed_date = row[0]
                                    db.update(connection, data.misc_data, "value", str(last_committed_date), condition="var = 'last_committed_state_date'")
                                    db.commit(connection)
                            elif len(c) > 1:
                                raise Warning("META database contains duplicate entries!")
                                exit()
                            current_table = "ID"+str(int(row[2]))
                            do_insert = False
                            if not db.if_table_exists(connection,current_table):
                                if log:
                                    print(logtab,'[NEWDATA] new state detected, creating table')
                                db.run(connection, "create table {} (date DATE, total_cases INT, total_deaths INT)".format(current_table))
                                last_committed_date = row[0]
                                db.update(connection, data.misc_data, "value", str(last_committed_date), condition="var = 'last_committed_state_date'")
                                db.commit(connection)
                                do_insert = True
                            else:
                                if len(db.query(connection, current_table, condition="date = '{}'".format(data.to_datetime(row[0])))) == 0:
                                    do_insert = True
                            c = db.query(connection,data.state_metadata,fieldset="percent_0to9",condition="FIPS = "+row[2])
                            if (len(c) == 0 or c[0][0] == None) and (int(row[2]) <= 56 or int(row[2] == 72)):
                                popdata = func_timeout(min_wait_time+10*current_tries,data.pull_population_data,args=[int(row[2])],kwargs={"log":log})
                                if not popdata.empty:
                                    for index, rowc in popdata.iterrows():
                                        db.update(connection,data.state_metadata,fieldset=
                                                  ["population",'percent_0to9', 'percent_10to19', 'percent_20to29', 'percent_30to39',
                                                  'percent_40to49', 'percent_50to59', 'percent_60to69', 'percent_70to79', 'percent_80plus'],
                                                  valueset=[rowc["population_size"], rowc['percent_0to9'], rowc['percent_10to19'], rowc['percent_20to29'],
                                                  rowc['percent_30to39'] ,rowc['percent_40to49'], rowc['percent_50to59'], rowc['percent_60to69'],
                                                  rowc['percent_70to79'], rowc['percent_80plus']],condition="FIPS = {}".format(row[2]))
                                        break
                            if do_insert:
                                db.insert(connection,current_table,[data.to_datetime(row[0]),int(row[3]),int(row[4])],formatt=["date","total_cases","total_deaths"])
                                if commit_every_insert:
                                    last_committed_date = row[0]
                                    db.update(connection, data.misc_data, "value", str(last_committed_date), condition="var = 'last_committed_state_date'")
                                    db.commit(connection)
                            #print("looped")
                    finish = True
                else:
                    if log:
                        print(logtab+"[ConnectionError] retrying...")
                    time.sleep(5)
                    current_tries+=1
            except Exception as eee:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                if log:
                    print(logtab,str(eee))
                    traceback.print_tb(exc_traceback, limit=1, file=sys.stdout)
                    print("retrying...")
                time.sleep(5)
                current_tries+=1
            except FunctionTimedOut as fto:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                if log:
                    print(logtab,str(fto))
                    traceback.print_tb(exc_traceback, limit=1, file=sys.stdout)
                    print("retrying...")
                time.sleep(5)
                current_tries+=1

        if finish:
            last_committed_date = last_updated_date
            db.update(connection, data.misc_data, "value", str(last_committed_date), condition="var = 'last_committed_date'")
            db.commit(connection)
            connection.close()
            if log:
                print("finished.")
        else:
            raise Warning("Connection to SQL Server lost.")

        return last_committed_date

    #accessors
    def get_county_data(fips,doprint=False,date="*"): # -> {str : (int, int) } # fun fact: if data exists for county on certain day, it is still uploaded to api, even if data is the exact same as the prior day's data. as such there are no skipped days. keep in mind when calcing new cases
        connection = db.create_connection(data.sql_addr, data.sql_user, data.sql_pass, data.db_name)
        table = "ID"+str(fips)
        if date == "*":
            dataraw = db.query(connection,table,"*")
        else:
            dataraw = db.query(connection,table,"*",condition="date = '{}'".format(date))
        dataclean = {}
        for dr in dataraw:
            if doprint:
                print(str(dr[0]), ": ",dr[1],", ", dr[2])
            dataclean[str(dr[0])] = (dr[1],dr[2])
        return dataclean

    def plot(fips, filename="generate",show=False,scale="log"): #scale = log, linear. include .png in filename
        #todo: enter fips as list of sets of (fips, bool numCases, bool numDeaths) for printing comparison graphs
        text = data.text_from_fips(fips)
        if filename == "generate":
            County, State = data.c_s_from_text(text)
            filename = "plot-"+County+"-"+State+".png".replace(" ","_")
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
    while True:
        userInput = input("[CMD] ")
        if userInput == "graph":
            location = input("[GRAPH] location: ")
            fips = data.fips_from_text(location)
            data.plot(fips,show=True,scale="log")
        elif userInput == "get":
            location = input("[GET] location: ")
            date = input("[GET] date: ")
            fips = data.fips_from_text(location)
            print(fips)
            #connection = db.create_connection(data.sql_addr, data.sql_user, data.sql_pass, data.db_name)

            print(db.print_all(db.query()))
            data.get_county_data(fips,date,doprint=True)
        elif userInput == "update":
            data.update_all(log=True,highlight_errors=True)
        elif userInput == "sql login":
            newUserName = input("[SQL] username: ")
            newPassword = input("[SQL] password: ")
            print("SQL login set for session. ")
    #data.init_db()
    #data.update_all(log=True,highlight_errors=True)
