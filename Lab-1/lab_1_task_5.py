from pyspark import SparkContext

sc = SparkContext(appName = "Lab-1_Task_5")
precipitation_file = sc.textFile("BDA/input/precipitation-readings.csv")
ostergotland_file = sc.textFile("BDA/input/stations-Ostergotland.csv")
plines = precipitation_file.map(lambda line: line.split(";"))
olines = ostergotland_file.map(lambda line: line.split(";"))

station = olines.map(lambda x: int(x[0]))
station = station.collect()
broadcastStation = sc.broadcast(station)

precipitation = plines.filter(lambda x: int(x[0]) in broadcastStation.value)
precipitation = precipitation.map(lambda x: ((x[1][0:4], x[1][5:7]), float(x[3])))
precipitation = precipitation.filter(lambda x: int(x[0][0]) >= 1993 and int(x[0][0]) <= 2016)
precipitation = precipitation.groupByKey()

avepre = precipitation.map(lambda x: (x[0][0], x[0][1], sum(x[1]) / len(station)))

avepre.saveAsTextFile("BDA/output")
