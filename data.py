import csv
import io
from urllib.request import urlopen
import mysql.connector
from mysql.connector import Error
from mysql.connector import errors
import datetime
import time

ny_api = "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv"

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

    def get_num_results(cursor):
        numResults = None
        try:
            numResults = len(cursor.fetchall())
            #outstring += " ({} RETURNED) ".format(str(len(cursor.fetchall())))
        except Error as ie:
            #print("caught error: "+str(ie))
            pass
        #print(numResults)
        if numResults == None:
            return 0
        else:
            return numResults

    def run(connection, cmd):
        cursor = connection.cursor(buffered=True)
        #cursor.close()
        #cursor = connection.cursor()
        try:
            cursor.execute(cmd)
            outstring = "\t[SUCCESS] Database exec'd successfully: {}".format(cmd)

            if db.log:
                print(outstring)

            return cursor

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

    def print_all(cursor):
        wasPrinted = False
        if cursor != None:
            for row in cursor:
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
def pull_from_census(log=False):
    
def pull_from_nyt(log=False):
    '''
    0    1       2       3   4       5
    date,county,state,fips,cases,deaths
    '''
    connection = db.create_connection("localhost", "miles", "shrek5", "covidbot")
    if connection != None:
        response = urlopen(ny_api)
        cr = csv.reader(line.decode() for line in response)
        for row in cr:
            if row[1]!="county":
                if log:
                    print(row)
                for i in range(1,3):
                    if "'" in row[i]:
                        if log:
                            print("\t[ERRORCATCH] detected invalid char in field")
                        #print(row[i])
                        row[i] = row[i].replace("'","\\'")
                        #print(row[i])
                if row[3] == '':
                    if log:
                        print("\t[ERRORCATCH] detected misinput")
                    c = db.query(connection,"META",fieldset="FIPS",condition=["state = '"+row[2]+"'","county = '"+row[1]+"'"])
                    if db.get_num_results(c) != 0:
                        row[3] = str(c.fetchall()[0][0])
                        if log:
                            print("\t[ERRORCATCH] error adjusted")
                    else:
                        if log:
                            print("\t[FAILURE] could not adjust, data skipped")
                        continue
                c = db.query(connection,"META",fieldset="FIPS",condition="FIPS = "+row[3])
                if db.get_num_results(c) == 0:
                    db.insert(connection,"META",[int(row[3]),row[1],row[2]],["FIPS","county","state"])
                elif db.get_num_results(c) > 1:
                    raise Warning("META database contains duplicate entries!")
                    exit()
                #else:
                #    (r_FIPS,r_county,r_state,r_pop) = c.fetchall()[0]
                current_table = "ID"+str(int(row[3]))
                do_insert = False
                result = db.if_table_exists(connection,current_table)
                #print(result)
                if not result:
                    db.run(connection, "create table {} (date DATE, total_cases INT, total_deaths INT)".format(current_table))
                    db.commit(connection)
                    do_insert = True
                else:
                    if db.get_num_results(db.query(connection, current_table, condition="date = '"+to_datetime(row[0])+"'")) == 0:
                        do_insert = True
                if do_insert:
                    db.insert(connection,current_table,[to_datetime(row[0]),int(row[4]),int(row[5])],formatt=["date","total_cases","total_deaths"])
                    #db.commit(connection)
                #print("looped")

        db.commit(connection)



if __name__ == "__main__":

    db.log = True
    refresh_db(log=True)
    #connection = db.create_connection("localhost", "miles", "shrek5", "covidbot")
    #print(db.if_table_exists(connection,"test"))
    #db.insert(connection,"test",[4,"coolestcounty","cooleststate"],["FIPS","county","state"])
    #db.commit(connection)
    #print(db.run(connection,"show tables").fetchall()[0][0])
    #db.print_all(db.query(connection,"test",fieldset="*"))
    #db.delete(connection,"test",condition="FIPS >= 0")
    #db.update(connection, "test", "population", 2, condition=["FIPS = 4"])
    #db.delete(connection,"test")
    #db.commit(connection)
    #print(to_datetime("2020-04-22"))
    #db.print_all(db.query(connection,"test",fieldset="*"))
    #print("loading...")
