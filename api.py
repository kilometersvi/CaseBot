import csv
from urllib.request import urlopen

ny_api = "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv"

class county:
    name = ""
    state = ""
    fips = 0
    cases = []
    deaths = []

    def __init__(self,name,state,fips):
        name = self.name
        state = self.state
        fips = self.fips

response = urlopen(ny_api)
cr = csv.reader(line.decode() for line in response)

for row in cr:
    print(row)
