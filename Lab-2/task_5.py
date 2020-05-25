from pyspark import SparkContext
from pyspark.sql  import SQLContext, Row

sparkContxt = SparkContext(appName="Lab-2_Task_5")
sqlContext = SQLContext(sparkContxt)

precipitationData = sparkContxt.textFile("BDA/input/precipitation-readings.csv")
stationsData = sparkContxt.textFile("BDA/input/stations-Ostergotland.csv")

precipLines = precipitationData.map(lambda line:line.split(";"))
precip_readings = precipLines.map(lambda x:Row(station = x[0],date=x[1],year=x[1].split("-")[0],month = x[1].split("-")[0:2],time=x[2],precip=float(x[3]),quality=x[4]))

stationLines = stationsData.map(lambda line: line.split(";"))
station_readings = stationLines.map(lambda x:Row(station=x[0]))

precipDataframe = sqlContext.createDataFrame(precip_readings)
precipDataframe.registerTempTable("precip_reading")

stationsDataframe = sqlContext.createDataFrame(station_readings)
stationsDataframe.registerTempTable("station_reading")

avg_monthly_precipitation = sqlContext.sql(
    """
    SELECT p.month, avg(p.precip_sum) as avg_monthly_precipitation
    FROM station_reading o
    INNER JOIN
    (SELECT station, month, sum(precip) AS precip_sum FROM precip_reading
    WHERE year >= 1993 AND year <= 2016
    GROUP BY station, month) p
    ON o.station = p.station
    GROUP BY p.month
    ORDER BY p.month
    """
)

avg_monthly_precipitation.rdd.saveAsTextFile("BDA/output/avgMonthlyPrecipitation")