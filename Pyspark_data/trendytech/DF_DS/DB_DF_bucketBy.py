# from pyspark import SparkConf
# from pyspark.sql import SparkSession
# my_conf = SparkConf()
# my_conf.set("spark.app.name", "my first application")
# my_conf.set("spark.master","local[*]")
# spark = SparkSession.builder.config(conf=my_conf).enableHiveSupport().getOrCreate()
# orderDf = spark.read.format("csv")\
#     .option("header",True)\
#     .option("inferSchema",True)\
#     .option("path","/Users/trendytech/Desktop/data/orders.csv").load()
#
# spark.sql("create database if not exists retail")
# orderDf.write.format("csv")\
#     .mode("overwrite")\
#     .bucketBy(4,"order_customer_id")\
#     .sortBy("order_customer_id")\
#     .saveAsTable("retail.orders4")
import boto3
from pyspark import SparkConf
from pyspark.sql import SparkSession
# s3_client=boto3.client('s3')
my_conf = SparkConf()
my_conf.set("spark.app.name", "my first application")
my_conf.set("spark.master","local[*]")
spark = SparkSession.builder.config(conf=my_conf).getOrCreate()
orderDf = spark.read.format("csv")\
    .option("header",True)\
    .option("inferSchema",True)\
    .option("path","/Users/trendytech/Desktop/data/AFI_ACCOUNTS_1_*.DAT").load()
target_bucket_name = 'target_bucketname'
orderDf.coalesce(1).write.mode('overwrite').parquet(target_bucket_name)
