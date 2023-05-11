from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
spark=SparkSession.builder.master("local").appName("keys").getOrCreate()

df1=spark.read.option("delimiter",",").csv("D:\Study\pyspark-docs\multi-folder-DATA/abc.txt")
df2=spark.read.option("delimiter",",").csv("D:\Study\pyspark-docs\multi-folder-DATA/abcd.txt")

listA=list(set(df1.columns)-set(df2.columns))
listB=list(set(df2.columns)-set(df1.columns))

for i in listA:
    df2.withColumn(i,lit('null'))

for j in listA:
    df1.withColumn(j,lit('null'))

df1.union(df2)    