from pyspark import SparkContext
from pyspark.sql import SQLContext,Row
from pyspark.sql import functions as func

sparkContxt = SparkContext(appName="Lab-2_Task_1")
sqlContext = SQLContext(sparkContxt)
#temperatureData = sparkContxt.textFile("BDA/input/temperature-readings.csv") 
temperatureData = sparkContxt.textFile("BDA/input/temperature-readings.csv") 
readLines = temperatureData.map(lambda line: line.split(";"))
tempDataFiltered = readLines.map(lambda x:Row(station = x[0],date=x[1],year=x[1].split("-")[0],time=x[2],temperature=float(x[3]),quality=x[4]))
tempDataFrame = sqlContext.createDataFrame(tempDataFiltered)
tempDataFrame.registerTempTable("tempDataFiltered")

tempDataFrame = tempDataFrame.filter(tempDataFrame['year']>=1950)
tempDataFrame = tempDataFrame.filter(tempDataFrame['year']<=2014)

#minimum and maximum temperature
min_temp = tempDataFrame.groupBy('year').agg(func.min('temperature').alias('temperature'))
min_temp = min_temp.join(tempDataFrame,['year','temperature']).select('year','station','temperature').orderBy(func.desc('temperature'))

max_temp = tempDataFrame.groupBy('year').agg(func.max('temperature').alias('temperature'))
max_temp = max_temp.join(tempDataFrame,['year','temperature']).select('year','station','temperature').orderBy(func.desc('temperature'))

min_temp.rdd.saveAsTextFile("BDA/output/min_temp")
max_temp.rdd.saveAsTextFile("BDA/output/max_temp")



import findspark
findspark.init()
sparkContxt = sparkContxt.stop()