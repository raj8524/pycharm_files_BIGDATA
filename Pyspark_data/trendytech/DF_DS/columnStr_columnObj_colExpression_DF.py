from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
my_conf = SparkConf()
my_conf.set("spark.app.name", "my first application")
my_conf.set("spark.master","local[*]")
spark = SparkSession.builder.config(conf=my_conf).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
orderDf =spark.read.option("header",True)\
    .option("inferSchema",True)\
    .csv("D://Study//TrendyTechInsight//week12spark//orders.csv")

# ==================================================== ------    column String
groupedDf = orderDf.repartition(4) \
    .where("customer_id > 10000") \
    .select("order_id","customer_id")

# groupedDf.show()
# ======================================================= -----  column Object
groupedDf1 = orderDf.repartition(4) \
    .where("customer_id > 10000") \
    .select(col("order_id"),col("customer_id"))

# groupedDf1.show()

# ===================================================column expression--
"""
groupedDf2 = orderDf.repartition(4) \
    .where("customer_id > 10000") \
    .select("order_id","customer_id","concat(order_status,'status')")
 groupedDf2.show()
"""
# concat("order_status","status") is expression. above expression is not coorect way to write.


groupedDf2 = orderDf.repartition(4) \
    .where("customer_id > 10000") \
    .select(col("order_id"),col("customer_id"),expr("concat(order_status,'status')"))
groupedDf2.show()

groupedDf3 = orderDf.repartition(4) \
    .where("customer_id > 10000") \
    .select("order_id","customer_id",expr("concat(order_status,'_status')"))
# groupedDf3.show(False)

groupedDf4 = orderDf.repartition(4) \
    .where("customer_id > 10000") \
    .selectExpr("order_id","customer_id","concat(order_status,'_status')")
# groupedDf4.show(False)