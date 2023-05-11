from pyspark.sql import SparkSession
from pyspark import SparkConf
my_conf=SparkConf()
my_conf.set("spark.app.name","my first app")
my_conf.set("spark.master","local[2]")
spark=SparkSession.builder.config(conf=my_conf).getOrCreate()
orders_df=spark.read.option("header",True).option("inferschema",True).csv("D:/Poojasparkcode/orders.csv")
grouped_Df=orders_df.repartition(4).where("order_customer_id > 1000").select("order_id","order_customer_id").groupby("order_customer_id").count()
grouped_Df.show()
# grouped_Df.printSchema()
spark.stop()