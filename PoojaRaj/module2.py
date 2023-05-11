import posixpath

from pyspark import SparkContext
from sys import stdin

sc = SparkContext("local[*]", "WordCount")
# sc.setLogLevel("ERROR")
input = sc.textFile("D:/Poojasparkcode/customerorders.csv")
rdd2=input.map(lambda x:(x.split(",")[0],float(x.split(",")[2])))
rdd3=rdd2.reduceByKey(lambda x,y:(x+y))
rdd4=rdd3.sortBy(lambda x:x[1],False)
result=rdd4.collect()
for a in result:
    print(a)