from pyspark import SparkContext

sc = SparkContext(appName = "Lab-1_Task_5")
precipitation_file = sc.textFile("BDA/input/precipitation-readings.csv")
ostergotland_file = sc.textFile("BDA/input/stations-Ostergotland.csv")

plines = precipitation_file.map(lambda line: line.split(";"))
olines = ostergotland_file.map(lambda line: line.split(";"))

precipLines= plines.filter(lambda x: int(x[1][0:4]) >= 1993 and int(x[1][0:4]) <= 2016)
stations = olines.map(lambda x: x[0])
stations = sc.broadcast(stations.collect())

precipData = precipLines.map(lambda x:((x[0],x[1][0:7]),float(x[3])))

#monthly precipitation
monthly_precipitation = precipData.reduceByKey(lambda x,y:x+y)

#filtering Ostergotland from monthly_precipitation and avg_precipitation
monthly_precipitation_filtered = monthly_precipitation.filter(lambda x:x[0][0] in stations.value)
monthly_precipitation_filtered = monthly_precipitation_filtered.map(lambda x:(x[0][1],(x[1],1)))
monthly_precipitation_agg = monthly_precipitation_filtered.reduceByKey(lambda x,y: (x[0] + y[0], x[1] + y[1]))
monthly_precipitation_avg = monthly_precipitation_agg.map(lambda x: (x[0], x[1][0] / x[1][1]))

monthly_precipitation_avg.saveAsTextFile("BDA/output/avg_monthly_precipitation")