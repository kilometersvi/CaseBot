import mysql.connector
from mysql.connector import Error
#from mysql.connector import errors
import time

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
        try:
            cursor = connection.cursor(buffered=True)
            cursor.execute(cmd)
            outstring = "\t[SUCCESS] Database exec'd successfully: {}".format(cmd)

            results = []
            errorResult = False
            try:
                results = cursor.fetchall()
            except Error as ie:
                errorResult = True
                pass

            if not errorResult:
                outstring += " ({} returned)".format(str(len(results)))

            if db.log:
                print(outstring)

            return results

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
        try:
            db.run(connection, cmd.format(*valueset))
        except mysql.connector.errors.IntegrityError as e:
            print("UploadError")
        #except _mysql_connector.MySQLInterfaceError as e:
        #    print("UploadError2")

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

    def get_column_names(connection, table):
        cmd = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table}'"
        r = db.run(connection, cmd)
        lis = []
        for s in r:
            lis.append(s[0])
        return lis

    def get_tables(connection,from_db=None,like=None,where=None):
        cmd = "SHOW TABLES "
        if from_db:
            cmd += "FROM "+from_db
        if where:
            cmd += db.where_gen(where)
        elif like:
            cmd += like
        db.run(connection, cmd)

    def print_all(results):
        wasPrinted = False
        for row in results:
            wasPrinted = True
            print(row)
        if not wasPrinted:
            print("empty set")

    def commit(connection):
        try:
            connection.commit()
            if db.log:
                print("\t[SUCCESS] Database committed successfully")
            time.sleep(0.1)
            return True
        except mysql.connector.errors as e:
            if db.log:
                print(f"[FAILURE] The error '{e}' occurredwhile committing.")
            raise Warning("[FAILURE] CommitError: "+e)
            return False
