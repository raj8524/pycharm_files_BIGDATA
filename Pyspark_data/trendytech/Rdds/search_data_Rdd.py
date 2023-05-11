from pyspark import SparkContext
from sys import stdin
sc=SparkContext("local[*]","searchdata")
sc.setLogLevel("ERROR")
readFile=sc.textFile("D://Study//TrendyTechInsight//scalaWeek9//search_data.txt")
splitLines=readFile.flatMap(lambda x: x.split(" "))
for x in splitLines.collect():
    print(x)
map_keypair=splitLines.map(lambda x :(x,1))
xy=map_keypair.reduceByKey(lambda x,y : x+y)
sortedValue1=xy.sortBy(lambda x:x[1],False)
finalCount=sortedValue1.collect()
# for i in finalCount:
#     print(i)