from pyspark import SparkContext
sc=SparkContext("local[*]","rating")
readFile=sc.textFile("D://Study//TrendyTechInsight//scalaWeek9//friendsdata.csv")
breakRdd=readFile.map(lambda x:(x.split("::")[2],(x.split("::")[3],1)))
# for k in breakRdd.collect():
#     print(k)
connectRdd=breakRdd.reduceByKey(lambda x,y:(int(x[0])+int(y[0]),x[1]+y[1]))
avg_age=connectRdd.mapValues(lambda x:x[0]/x[1]).sortBy(lambda x:x[1],False)
for k in avg_age.collect():
    print(k)

