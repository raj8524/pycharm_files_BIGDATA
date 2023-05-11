

""" Exercise 1
from pyspark import SparkContext
sc=SparkContext("local[*]","courseid")
readFile=sc.textFile("D://Study//TrendyTech Insight//Spark week 10//dataset//chapters.csv")
splitFile=readFile.map(lambda x:(x.split(",")[1],x.split(",")[0]))
course_chapter=splitFile.groupByKey().map(lambda x:(x[0],len(x[1])))
for i in course_chapter.collect():
    print(i)
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,IntegerType,DateType,StructField
spark=SparkSession.builder.master("local").appName("keys").getOrCreate()
schema=StructType([StructField("userID",IntegerType(),True),StructField("chapterId",IntegerType(),True),StructField("timestamp",DateType(),True)])
df=spark.read.option("delimiter",",").schema(schema).csv("D://Study//TrendyTech Insight//Spark week 10//dataset//views1.csv")
print(df.show(20))

