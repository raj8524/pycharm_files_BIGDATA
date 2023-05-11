from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession
import logging
my_conf = SparkConf()
my_conf.set("spark.app.name", "my first application")
my_conf.set("spark.master","local[*]")

spark = SparkSession.builder.config(conf=my_conf).enableHiveSupport().getOrCreate()

orderDf = spark.read.format("csv")\
    .option("header",True)\
    .option("inferSchema",True)\
    .option("path","D:/Study/TrendyTechInsight/week12spark/orders.csv")\
    .load()

spark.sql("create database if not exists retail")

orderDf.write.format("csv")\
    .mode("overwrite")\
    .bucketBy(4,"order_customer_id")\
    .sortBy("order_customer_id")\
    .saveAsTable("retail.orders4")