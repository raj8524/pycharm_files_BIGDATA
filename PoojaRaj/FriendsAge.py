from pyspark import SparkContext
from sys import stdin

def parseLine(line):
    fields=line.split("::")
    age=int(fields[2])
    num_friends=int(fields[3])
    return (age,num_friends)

sc = SparkContext("local[*]", "friendsbyage")
sc.setLogLevel("ERROR")
input = sc.textFile("D:/Poojasparkcode/friendsdata.csv")
# rdd1=input.map(lambda x:(int(x.split(",")[2]),int(x.split(",")[3])))
# rdd2=rdd1.map(lambda x:(x,(x[1],1)))
rdd1=input.map(parseLine)
rdd2=rdd1.mapValues(lambda x:(x,1))
rdd3=rdd2.reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))
rdd4=rdd3.mapValues(lambda x:x[0]/x[1])
rdd5=rdd4.sortBy(lambda x:x[1],False)
result=rdd5.collect()
for a in result:
    print(a)