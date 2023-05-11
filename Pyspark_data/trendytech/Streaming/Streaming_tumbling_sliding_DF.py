from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, window
from pyspark.sql.types import *
from pyspark.sql import functions as F
spark = SparkSession.builder \
        .master("local[2]") \
        .appName("Tumbling") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.shuffle.partitions", 3) \
        .getOrCreate()
# 1. read
ordersDf = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", "12345") \
        .load()
# define own schema instead of infering it
orderSchema = StructType([StructField("order_id", IntegerType()),
        StructField("order_date", StringType()),
        StructField("order_customer_id", IntegerType()),
        StructField("order_status", StringType()),
        StructField("amount", IntegerType())])
# 2. process
valueDF = ordersDf.select(from_json(F.col("value"),orderSchema).alias("value"))
refinedOrderDF = valueDF.select("value.*")
windowAggDF = refinedOrderDF \
        .withWatermark("order_date","30 minute") \
        .groupBy(window(F.col("order_date"), "15 minute","1 minute")) \
        .agg(F.sum("amount").alias("totalInvoice"))

opDf = windowAggDF.select("window.start", "window.end", "totalInvoice")
ordersQuery = opDf.writeStream \
        .format("console") \
        .outputMode("update") \
        .option("checkpointLocation", "D://software_installation//Pycharm_files//Pyspark_data//trendytech//checkpoint") \
        .trigger(processingTime="15 second")\
        .start()

ordersQuery.awaitTermination()