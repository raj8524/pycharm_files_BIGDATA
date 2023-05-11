from pyspark.sql.functions import column
from pyspark.sql.functions import lit
from pyspark import SparkConf
from pyspark.sql import SparkSession
my_conf=SparkConf()
my_conf.set("spark.app.name","my first application")
my_conf.set("spark.master","local[*]")
spark=SparkSession.builder.config(conf=my_conf).getOrCreate()
df1=spark.read.format("csv").option("header",True).option("inferSchema",True).option("path","D:/Study/Final_LP.csv").load()
# this input has duplicates, we need to dedup(drop duplicates?)
df2=df1.dropDuplicates()
# ========================
# remove blank rows
df3=df1.dropna("all")
# ============================
# Remove column named Catalog, permissiongrantable
df4=df3.drop("Catalog","permissiongrantable")
# Add column (data type string) named Aws_Account_Id with default value as 935284207569
df4.withColumn("Aws_Account_Id",lit("935284207569")).show()

