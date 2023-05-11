from pyspark import SparkContext
sc=SparkContext("local[*]","reduce_group")
sc.setLogLevel("ERROR")
print(sc.defaultParallelism)
print(sc.defaultMinPartitions)
readLines=sc.textFile("D://Study//TrendyTechInsight//Sparkweek10//bigLog.txt")
print(readLines.getNumPartitions())
# finalCount=readLines.map(lambda x:(x.split(":")[0],x.split(":")[1])).groupByKey().map(lambda x:(x[0],len(x[1]))).collect()
finalCount=readLines.map(lambda x:(x.split(":")[0],1))\
    .reduceByKey(lambda x,y:x+y)\
    .collect()

for k in finalCount:
    print(k)

finalCount1=readLines.mapValues(lambda x:(x.split(":")[0],1))\
    .reduceByKey(lambda x,y:x+y)\
    .collect()
for k in finalCount1:
    print(k)
