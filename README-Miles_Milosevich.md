Miles Milosevich
# Database #

## First, a bit about FIPS codes ##
FIPS codes (Federal Information Processing Standard) are used to ID different regions throughout the US. These codes can narrow down between the state, county, and city ('place') level. A state code is 2 digits long, county code is 5 (state code + 3 digit identifier), and a city is 7 (state code + 5 digit identifier). These IDs, while not only convenient in this use case, are used to retrieve population data from the US National Census. Although cities are not often used, in special cases where a city extends beyond the borders of its county, as is the case with New York City and Kansas City, its city code can be used to retrieve its data.

## Functional Requirements ##
For the purposes of this bot, a database is required in order to perform the following functions:
* Work with FIPS codes to store case/death info by sector, divided into the state, county, and city type. This is performed by creating a table for each relevant place, found by its associated code, which will store the total cases and total deaths in the area by date. Cases marked as 'Unknown County' are provided their own county FIPS code, 999.
* Update COVID data from the New York Times github repo, with safety checks in case of network interruption. In order to prevent duplicate entries and excessive sql calls, the database will keep track of the last date it received case data for, effectively causing the db to only pull new case data. In case of network interrupts, the update will wait and try again.
* Retrieve population data for each area from the 2015 US National Census, as well as percentage of each age group in a given population, to be used with calculating estimated true spread of the disease. In order to save storage space, census data is only pulled once a population has reached a threshold number of deaths, as true spread cannot be calculated in regions without enough data.
* Store Subscriber data. As users are to be allowed to subscribe to the bot to get personalized updates on cases in their area, this data must be stored as well. 
* Updatable and Reproducable. Database can initialize itself from a fresh SQL server.

## Implementation ##
script/database contains several files:
* sql.py: Construct SQL commands for specific functions. General use.
* data.py: Maintain and retrieve from COVID database. Contains simple command line if run as main, with commands "graph", "get", "update". Location input provided as either FIPS code or "{County}, {State}".
* userbase.py: Maintain and retrieve from subscriber userbase.
* api.py: Contains class for storing data for individual county. Currently not used but may be used for further statistics.

## Dependencies ##
Python packages:
* CensusData
* mysql-connector-python
* matplotlib
* pandas
* numpy
* csv
* func-timeout

APIs/Repositories:
* [New York Times COVID-19 Database](https://github.com/nytimes/covid-19-data)
* US 2015 National Census Data

## Create Your Own COVID-19 SQL Database ##
Want to test this database? 
1. Install MySQL Server on system. Skip if already installed. DB will create new database in server for you.
2. Install dependent python packages with pip3.
3. Run script/database/data.py as main. Python version 3.7 required.
4. Enter cmd 'sql login' to set sql login for session.
5. Enter cmd 'update' to begin pulling data.
6. Enter cmd 'graph' or 'get' at your leisure. When asking for location, provide FIPS code or "{County}, {State}". The word "County" within county is invalid. Case sensitive. When asking for date, enter as "YYYY-MM-DD".

## About This Data ##
As this data is pulled from a third party source, this project has no guarantees to its validity. 
NYT Collects this data from hospitals accross the nation, collected data by county, state, and place across all states and US territories.
Worth noting, however, is that at an unspecified points in time, hospitals across the nation quietly switched from reporting confirmed cases/deaths to including probable cases/deaths within the same tally and no indestinguishing tags. As such, this dataset contains both confirmed and probable cases/deaths. [Read here for more](https://github.com/nytimes/covid-19-data/blob/master/PROBABLE-CASES-NOTE.md).

# Statistics #
*Incomplete at time of writing.*

## Functional Reqs ##
Statistical tools that need to be used:
* Estimate the number of true cases based on the death rate.

## Implementation ##
Lauchtman et. all's work would find a similar goal to ours, all that was left was to use our database with their calculations.

## Dependencies ##
Python packages:
* matplotlib
* numpy
* pandas
* sklearn
* plotly
* statistics
* requests

APIs/Repositories:
* Lachtman et. al, Correcting under-reported COVID-19 casenumbers: estimating the true scale of the pandemic ([MedRx Paper](https://www.medrxiv.org/content/10.1101/2020.03.14.20036178v2.full.pdf#cite.liu2020reproductive), [Github](https://github.com/lachmann12/covid19/blob/master/script/prediction.py))


## To-Do ##
* fix library errors with Lachtman dependencies
* update plot() to handle multiple fips with params for which attributes to print for better comparisons between regions
* better error handling for invalid FIPS/location input
