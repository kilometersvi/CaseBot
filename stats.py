import scipy
from math import e
from sql import db

class calc:
    #CFR of South Korea
    CFRb = 0.01635
    CFRb_update = "2020-03-28"

    def EDG(t, a, b): #EDG(t) = a×e^((−b×t)+1)
        return pow(e,-1*b+1*t)*a

    def PredictedDeaths(t, d, a, b): #PD(t) = PD(t−1)×EDG(t+d)
        if t>1:
            return calc.PredictedDeaths(t-1, d) * calc.EDG(t + d, a, b)
        else:
            return calc.EDG(t + d, b)

    def VulnerabilityFactor(fips):
        table = "ID"+fip
        connection = db.create_connection(data.sql_addr, data.sql_user, data.sql_pass, data.db_name)
        popdata = db.query(connection,table,"population,pop0to9,pop10to19,pop20to29,pop30to39,pop40to49,pop50to59,pop60to69,pop70to79,pop80plus")

        fr_by_age = {0:0,10:0,20:0,30:0.11,40:0.09,50:0.37,60:1.51,70:5.35,80:10.84}
        for age in range(0,81,10):
            res = db.query()

    def EC():
        pass

    def plot(fips, filename="generate",show=False,scale="log"): #scale = log, linear. include .png in filename
        #todo: enter fips as list of sets of (fips, bool numCases, bool numDeaths) for printing comparison graphs
        text = data.text_from_fips(fips)
        if filename == "generate":
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
    calc.VulnerabilityFactor(0)
    #fips = data.fips_from_text("Contra Costa, California")
    #data.plot(fips,show=True,scale="log")
