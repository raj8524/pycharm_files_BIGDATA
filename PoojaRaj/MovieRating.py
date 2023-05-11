from pyspark import SparkContext
from sys import stdin

sc = SparkContext("local[*]", "MovieRating")
sc.setLogLevel("ERROR")
input = sc.textFile("D:/Poojasparkcode/moviedata.data")
rdd1=input.map(lambda x:(x.split("\t")[2]))
rdd2=rdd1.map(lambda x:(x,1))
rdd3=rdd2.reduceByKey(lambda x,y:(x+y))
rdd4=rdd3.sortBy(lambda x:x,False)
result=rdd4.collect()
for a in result:
    print(a)