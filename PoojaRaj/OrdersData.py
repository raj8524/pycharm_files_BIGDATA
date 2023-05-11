from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, StringType

from pyspark import SparkConf
from pyspark.sql import SparkSession
my_conf=SparkConf()
my_conf.set("spark.app.name","my first application")
my_conf.set("master.name","local[*]")
ordersSchema=StructType([
    StructField("orderid",IntegerType()),
    StructField("orderdate",TimestampType()),
    StructField("customerid",IntegerType()),
    StructField("status",StringType())
])
spark=SparkSession.builder.config(conf=my_conf).getOrCreate()
order_df=spark.read.format("csv").option("header",True).schema(ordersSchema).option("path","D:/Poojasparkcode/orders.csv").load()
order_ds=order_df.repartition(4).where("customerid > 1000").select("orderid","customerid").groupby("customerid").count()
order_ds.show()
spark.stop()