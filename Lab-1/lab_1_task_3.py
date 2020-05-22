from pyspark import SparkContext

def minMaxData(temp):
    return max(temp)+min(temp)

sparkContxt = SparkContext(appName="Lab-1_Task_3") #Name of the job
temperatureData = sparkContxt.textFile("BDA/input/temperature-readings.csv") 
readLines = temperatureData.map(lambda line: line.split(";"))
stationTemperature = readLines.map(lambda x:((x[1][0:4],x[1][5:7],x[1][8:10],x[0]),float(x[3])))
stationTemperature = stationTemperature.filter(lambda x:int(x[0][0])>=1960 and int(x[0][0])<=2014)
averageTemp = stationTemperature.groupByKey()
averageTemp = averageTemp.map(lambda x: ((x[0][0],x[0][1],x[0][3]),minMaxData(x[1])))
averageTemp = averageTemp.groupByKey()
averageTemp = averageTemp.map(lambda x:(x[0][0],x[0][1],x[0][2],sum(x[1])/62))
averageTemp.saveAsTextFile("BDA/output")


