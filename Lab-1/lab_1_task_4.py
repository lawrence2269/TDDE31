from pyspark import SparkContext

sc = SparkContext(appName = "Lab-1_Task_4")
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
precipitation_file = sc.textFile("BDA/input/precipitation-readings.csv")
tlines = temperature_file.map(lambda line: line.split(";"))
plines = precipitation_file.map(lambda line: line.split(";"))

temperature = tlines.map(lambda x: (x[0], float(x[3])))
temperature = temperature.groupByKey()
temperature = temperature.map(lambda x: (x[0], max(x[1])))
temperature = temperature.filter(lambda x: x[1] >= 25 and x[1] <= 30)

precipitation = plines.map(lambda x: ((x[0], x[1][0:4], x[1][5:7], x[1][8:10]), float(x[3])))
precipitation = precipitation.groupByKey()
precipitation = precipitation.map(lambda x: (x[0][0], sum(x[1])))
precipitation = precipitation.filter(lambda x: x[1] >= 100 and x[1] <= 200)

tp = temperature.join(precipitation)
tp.saveAsTextFile("BDA/output")
