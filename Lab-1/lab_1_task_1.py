from pyspark import SparkContext

def maximumTemperature(a,b):
    if(a>=b):
        return a
    else:
        return b

def minimumTemperature(a,b):
    if(a<=b):
        return a
    else:
        return b
    
sparkContxt = SparkContext(appName="Lab-1_Task_1") #Name of the job
temperatureData = sparkContxt.textFile("BDA/input/temperature-readings.csv") 
readLines = temperatureData.map(lambda line: line.split(";"))
year_temperature = readLines.map(lambda x:(x[1][0:4],float(x[3])))
year_temperature = year_temperature.filter(lambda x:int(x[0])>=1950 and int(x[0])<=2014)
max_temperatures = year_temperature.reduceByKey(maximumTemperature)
min_temperatures = year_temperature.reduceByKey(minimumTemperature)
max_temperature_sorted = max_temperatures.sortBy(ascending=False, keyfunc = lambda x: x[1])
min_temperature_sorted = min_temperatures.sortBy(ascending=False, keyfunc = lambda x: x[1])
max_temperature_sorted.saveAsTextFile("BDA/output")
min_temperature_sorted.saveAsTextFile("BDA/output")



