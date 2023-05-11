from pyspark.sql import SparkSession
spark = SparkSession.builder \
        .master("local[2]") \
        .appName("My Streaming Application") \
        .getOrCreate()
# read from the stream
linesDf = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", "1234") \
        .load()
linesDf.printSchema()
# process
wordsDf=linesDf.selectExpr("explode(split(value,' ')) as word")
countsDf =wordsDf.groupBy("word").count()

# write to the sink
wordCountQuery = countsDf.writeStream \
        .format('console') \
        .outputMode('complete') \
        .option("checkpointLocation","checkpoint-location1") \
        .start()

wordCountQuery.awaitTermination()