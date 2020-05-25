from pyspark import SparkContext
from pyspark.sql  import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext()
sqlContext = SQLContext(sc)

rdd = sc.textFile("BDA/input/temperature-readings.csv")
parts = rdd.map(lambda l: l.split(";"))

tempReadings = parts.map(lambda p: Row(
station = p[0],
date = p[1],
year = p[1].split("-")[0], month = p[1].split("-")[1], day = p[1].split("-")[2],
time = p[2],
value = float(p[3]),
quality = p[4]))

schemaTempReadings = sqlContext.createDataFrame(tempReadings)
schemaTempReadings.registerTempTable("tempReadings")

schemaTempReadings = schemaTempReadings.filter(schemaTempReadings['year'] >= 1960)
schemaTempReadings = schemaTempReadings.filter(schemaTempReadings['year'] <= 2016)

averageTemp = schemaTempReadings.groupBy('year', 'month', 'station').agg(
F.avg('value').alias('avg')).orderBy(F.desc('avg')) #Monthly avg

averageTemp.rdd.saveAsTextFile("BDA/output/avg_monthly_temp")
