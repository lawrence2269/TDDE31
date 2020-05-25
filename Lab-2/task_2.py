from pyspark import SparkContext
from pyspark.sql import SQLContext,Row

sparkContxt = SparkContext(appName="Lab-2_Task_2")
sqlContext = SQLContext(sparkContxt)
temperatureData = sparkContxt.textFile("BDA/input/temperature-readings.csv") 
readLines = temperatureData.map(lambda line: line.split(";"))
tempDataFiltered = readLines.map(lambda x:Row(station = x[0],date=x[1],year=x[1].split("-")[0],month = x[1].split("-")[1],time=x[2],temperature=float(x[3]),quality=x[4]))
tempDataFrame = sqlContext.createDataFrame(tempDataFiltered)
tempDataFrame.registerTempTable("tempDataFiltered")

tempDataFrame = tempDataFrame.filter(tempDataFrame['year']>=1950)
tempDataFrame = tempDataFrame.filter(tempDataFrame['year']<=2014)
tempDataFrame = tempDataFrame.filter(tempDataFrame['temperature']>=10)

temp_count = sqlContext.sql("""
    SELECT year, month, count(temperature) as temps_counts
    FROM tempDataFiltered 
    WHERE year >= 1950 AND year <= 2014 AND value > 10 
    GROUP BY year, month
    ORDER BY temps_counts DESC                            
""")

distinct_count = sqlContext.sql("""
    SELECT year, month, COUNT(station)
    FROM
    (SELECT DISTINCT year, month, station
    FROM tempDataFiltered 
    WHERE year >= 1950 AND year <= 2014 AND temperature > 10) dt
    GROUP BY year, month
""")

temp_count.rdd.saveAsTextFile("BDA/output/temp_count")
distinct_count.rdd.saveAsTextFile("BDA/output/distinct_count")