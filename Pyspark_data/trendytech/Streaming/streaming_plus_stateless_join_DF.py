from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, window,col
from pyspark.sql.types import *
spark = SparkSession.builder \
        .master("local[2]") \
        .appName("join") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.shuffle.partitions", 3) \
        .getOrCreate()
transactionSchema= StructType([(StructField("card_id", LongType())),
    StructField("amount", IntegerType()),
    StructField("postcode", IntegerType()),
    StructField("pos_id", LongType()),
    StructField("transaction_dt", TimestampType())])

   # read streaming
transactionsDf = spark.readStream\
    .format("socket")\
    .option("host","localhost")\
    .option("port","1234")\
    .load()

  # process
valueDF = transactionsDf.select(from_json(col("value"),
    transactionSchema).alias("transaction"))
# valueDF.printSchema()
refinedTransactionDF = valueDF.select("transaction.*")
# refinedTransactionDF.printSchema()

# load static DF

memberDF=spark.read\
    .format("csv").option("header",True).option("inferSchema",True)\
    .option("path","D://Study//TrendyTechInsight//week16Streaming//member_details.csv").load()

joinExpr=refinedTransactionDF["card_id"]==memberDF["card_id"]

enrichDF=refinedTransactionDF.join(memberDF,joinExpr,"inner")\
               .drop(memberDF["card_id"])

# write to sink
transactionQuery = enrichDF.writeStream\
    .format("console")\
    .outputMode("update")\
    .option("checkpointLocation","D://software_installation//Pycharm_files//Pyspark_data//trendytech//checkpoint")\
    .trigger(processingTime="15 second")\
    .start()

transactionQuery.awaitTermination()