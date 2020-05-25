from pyspark import SparkContext
from pyspark.sql  import SQLContext, Row

sparkContxt = SparkContext(appName="Lab-2_Task_4")
sqlContext = SQLContext(sparkContxt)

temperatureData = sparkContxt.textFile("BDA/input/temperature-readings.csv") 
precipitationData = sparkContxt.textFile("BDA/input/precipitation-readings.csv")

tempLines = temperatureData.map(lambda line: line.split(";"))
temp_readings = tempLines.map(lambda x:Row(station = x[0],date=x[1],year=x[1].split("-")[0],time=x[2],temperature=float(x[3]),quality=x[4]))

precipLines = precipitationData.map(lambda line:line.split(";"))
precip_readings = precipLines.map(lambda x:Row(station = x[0],date=x[1],year=x[1].split("-")[0],time=x[2],precip=float(x[3]),quality=x[4]))

tempDataframe = sqlContext.createDataFrame(temp_readings)
tempDataframe.registerTempTable("temp_reading")

precipDataframe = sqlContext.createDataFrame(precip_readings)
precipDataframe.registerTempTable("precip_reading")

precipitation_filter = sqlContext.sql(
    """
    SELECT station, MAX(daily_precip) AS max_precip
    FROM
    (SELECT station, SUM(precip) AS daily_precip FROM precip_reading
    GROUP BY station, date) dt
    GROUP BY station
    HAVING MAX(daily_precip) >= 100 AND MAX(daily_precip) <= 200
    """
)

precipitation_filter.registerTempTable("precip_filtered_readings")

temperature_filter = sqlContext.sql(
    """
    SELECT station, MAX(temperature) AS max_temp
    FROM temp_reading
    GROUP BY station
    HAVING MAX(temperature) >= 25 AND MAX(temperature) <= 30
    """
)

temperature_filter.registerTempTable("temp_filtered_readings")

combined_result = sqlContext.sql(
    """
    SELECT t.station, t.max_temp, p.max_precip FROM
    temp_filtered_readings t
    INNER JOIN precip_filtered_readings p
    ON t.station = p.station
    """    
)

combined_result.rdd.saveAsTextFile("BDA/output/maxDailyPrecipitation")
