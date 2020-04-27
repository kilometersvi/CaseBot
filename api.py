import csv
import io
from urllib.request import urlopen
import mysql.connector
from mysql.connector import Error
from sql import db

ny_api = "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv"

#deprecated, may be useful later tho. see data.py
class County:
    def __init__(self,name,state,fips):
        self.name = name
        self.state = state
        self.fips = fips
        self.cases = []
        self.deaths = []


    def __getattr__(self,name):
        if(name == "lenCases"):
            return len(self.cases)
        elif(name == "lenDeaths"):
            return len(self.deaths)
        elif(name == "Cases"):
            print(type(self.cases))
            if self.cases == None:
                self.cases = []
            return self.cases
        elif(name == "Deaths"):
            if self.deaths == None:
                self.deaths = []
            return self.deaths

    def getTotalNumCases(self):
        return self.cases[-1].num

    def getTotalNumDeaths(self):
        return self.deaths[-1].num

    def __str__(self):
        return c.name+", "+c.state+" ("+c.fips+")"

    def getData(self):
        return ""+str(c)+": \n\tCases: "+str(self.getTotalNumCases())+"\n\tDeaths: "+str(self.getTotalNumDeaths())

    def __eq__(self,other):
        if other != None:
            return (self.getTotalNumDeaths() == other.getTotalNumDeaths())
        return False

    def __ne__(self,other):
        if other != None:
            return (self.getTotalNumDeaths() != other.getTotalNumDeaths())
        return False

    def __lt__(self,other):
        if other != None:
            return (self.getTotalNumDeaths() < other.getTotalNumDeaths())
        return False

    def __le__(self,other):
        if other != None:
            return (self.getTotalNumDeaths() <= other.getTotalNumDeaths())
        return False

    def __gt__(self,other):
        if other != None:
            return (self.getTotalNumDeaths() > other.getTotalNumDeaths())
        return False

    def __ge__(self,other):
        if other != None:
            return (self.getTotalNumDeaths() >= other.getTotalNumDeaths())
        return False


class DateNum:
    def __init__(self,date,num):
        self.date = date
        self.num = num

def findCounty(countyName,state,fips=None):
    if fips == None:
        for c in data:
            if c.name == countyName and c.state == state:
                return c
        #print("could not find")
        return None
    else:
        for c in data:
            if c.fips == fips:
                return c
        #print("could not find")
        return None

print("loading...")
response = urlopen(ny_api)
cr = csv.reader(line.decode() for line in response)

global data
data = []

#f = open("outfile.txt","w")


'''
0    1       2       3   4       5
date,county,state,fips,cases,deaths
'''

for row in cr:
    if(row[1]!="county"):
        c = findCounty("","",row[3])
        if c == None:
            #print("did not exist")
            c = County(row[1],row[2],row[3])
            data.append(c)
        else:
            pass#print("did exist")
        #bstr = c.name+","+row[4]+","+row[5]
        #print(bstr)
        c.cases.append(DateNum(row[0],int(row[4])))
        c.deaths.append(DateNum(row[0],int(row[5])))
        #print("looped")

def printAll():
    for c in data:
        nstr = c.name+", "+c.state+" ("+c.fips+"): \t\t\t\tcases: "+str(c.getTotalNumCases())+"\tdeaths: "+str(c.getTotalNumDeaths())
        print(nstr)
def calcBounds():
    longest_county = 0
    longest_state = 0
    for c in data:
        if len(c.name) > longest_county:
            longest_county = len(c.name)
        if len(c.state) > longest_state:
            longest_state = len(c.state)
    print("county: ",longest_county,", state: ",longest_state)

def getCountyData(name, state, fips=None):
    c = None
    if fips == None:
        c = findCounty(name,state)
    else:
        c = findCounty("","",fips)
    nstr = "Data for "+c.getData()
    print(nstr)


if __name__ == "__main__":

    sorted(data)
    running = True
    while running:
        user_in = input("cmd: ")
        args = user_in.split(" ")
        if args[0]=="printAll":
            printAll()
        elif args[0]=="getCountyData":
            if args[1]=="byFips":
                user_fips = input("fips: ")
                getCountyData("","",str(user_in))
            elif args[1]=="byName":
                user_county = input("county: ")
                user_state = input("state: ")
                getCountyData(user_county,user_state)
        elif args[0]=="calcBounds":
            calcBounds()
        elif args[0]=="exec":
            user_code = input("code: ")
            exec(user_code)
        elif args[0]=="exec_db":
            user_code = input("sql: ")
            db.run(connection,user_code)
