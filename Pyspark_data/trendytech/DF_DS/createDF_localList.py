from pyspark import SparkConf
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
my_conf = SparkConf()
my_conf.set("spark.app.name", "my first application")
my_conf.set("spark.master","local[*]")
spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

myList = [(1,"2013-07-25",11599,"CLOSED"),
    (2,"2014-07-25",256,"PENDING_PAYMENT"),
    (3,"2013-07-25",11599,"COMPLETE"),
    (4,"2019-07-25",8827,"CLOSED")]

ordersDf = spark.createDataFrame(myList)\
    .toDF("orderid","orderdate","customerid","status")

newDf = ordersDf\
    .withColumn("date1",unix_timestamp(col("orderdate"))) \
    .withColumn("newid", monotonically_increasing_id()) \
    .dropDuplicates(["orderdate","customerid"])\
    .drop("orderid")\
    .sort("orderdate")

ordersDf.printSchema()
ordersDf.show()
newDf.show()
