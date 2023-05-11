from pyspark import SparkContext
from sys import stdin
sc = SparkContext("local[*]", "searchdata")
sc.setLogLevel("ERROR")
readFile = sc.textFile("D://Study//TrendyTechInsight//Sparkweek10//bigdatacampaigndata.csv")
selected_cols = readFile.map(lambda y: (float(y.split(',')[10]), y.split(',')[0]))
splitString = selected_cols.flatMapValues(lambda z: (z.split(" ")))
changeindex = splitString.map(lambda x: (x[1], x[0]))
reduceSum = changeindex.reduceByKey(lambda y,z: y + z)
for i in reduceSum.sortBy(lambda x: x[1], False).take(30):
    print(i)
