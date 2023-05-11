"""
from pyspark import SparkContext
sc=SparkContext("local[*]","blankLine")
def blankLineCount(line):
    if (len(line)==0):
        myaccu.add(1)
readLines=sc.textFile("D://Study//TrendyTech Insight//Spark week 10//sample.txt")
myaccu=sc.accumulator(0)
readLines.foreach(blankLineCount)
print(myaccu.value)

"""
import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("accumulator").getOrCreate()
accum=spark.sparkContext.accumulator(0)
rdd=spark.sparkContext.parallelize([1,2,3,4,5])
rdd.foreach(lambda x:accum.add(x))
print(accum.value)

