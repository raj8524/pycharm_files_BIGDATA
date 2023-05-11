from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, window
from pyspark.sql.types import *
from pyspark.sql import functions as F
spark = SparkSession.builder \
    .master("local[2]") \
    .appName("My Streaming Application") \
    .config("spark.sql.shuffle.partitions",3) \
    .config("spark.streaming.stopGracefullyOnShutdown","true") \
    .config("spark.sql.streaming.schemaInference","true") \
    .getOrCreate()
# read from file source
ordersDf = spark.readStream \
    .format("json") \
    .option("path", "myinputfolder") \
    .option("maxFilesPerTrigger",1) \
    .load()
# process
ordersDf.createOrReplaceTempView("orders")
completeOrders =spark.sql("select * from orders where order_status='COMPLETE'")
# write to the sink
wordCountQuery = completeOrders.writeStream \
    .format('json') \
    .outputMode('append') \
    .option("path", "myoutputfolder") \
    .option("checkpointLocation","checkpoint-location5") \
    .trigger(processingTime='30 seconds').start()

wordCountQuery.awaitTermination()

