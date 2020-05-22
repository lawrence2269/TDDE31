from pyspark import SparkContext

sparkContxt = SparkContext(appName="Lab-1_Task_2a") #Name of the job
temperatureData = sparkContxt.textFile("BDA/input/temperature-readings.csv") 
readLines = temperatureData.map(lambda line: line.split(";"))
year_temperature = readLines.map(lambda x:((x[1][0:4],x[1][5:7]),float(x[3])))
year_temperature = year_temperature.filter(lambda x: int(x[0][0])>=1950 and int(x[0][0])<=2014 and x[1]>=10)
temperature_count = year_temperature.groupByKey()
temperature_count = temperature_count.map(lambda x:(x[0][0],x[0][1],len(x[1])))
temperature_count.saveAsTextFile("BDA/output")