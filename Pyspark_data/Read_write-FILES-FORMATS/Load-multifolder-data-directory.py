#to read data from multi-folder from directory
import re
from pyspark.sql import SparkSession
spark=SparkSession.builder.master("local").appName("keys").getOrCreate()

df=spark.read.option("delimiter",",").csv("D:\Study\pyspark-docs\multi-folder-DATA/data*/abc*.txt")
# df.show()
p="D:\Study\pyspark-docs\multi-folder-DATA"

df4=spark.read.option("delimiter",",").csv(["p/data1/*.txt","p/data2/*.txt"])
df4.show()

#this loads data only from data1,data2,data3
df2=spark.read.option("delimiter",",").csv("D:\Study\pyspark-docs\multi-folder-DATA/data[1-3]*/*.txt")
# df2.show()
#this loads data only from data1,data2,data3
df3=spark.read.option("delimiter",",").csv("D:\Study\pyspark-docs\multi-folder-DATA/data{1,2,5}*/*.txt")
# df3.show()