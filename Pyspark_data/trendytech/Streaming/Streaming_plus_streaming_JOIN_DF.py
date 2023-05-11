from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, window
from pyspark.sql.types import *
from pyspark.sql import functions as F
spark = SparkSession.builder \
        .master("local[2]") \
        .appName("join") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.shuffle.partitions", 3) \
        .getOrCreate()
# define own schema instead of infering it
impressionSchema = StructType([
        StructField("impressionID", StringType()),
        StructField("ImpressionTime", TimestampType()),
        StructField("CampaignName", StringType()),
        ])
clickSchema = StructType([
        StructField("clickID", StringType()),
        StructField("ClickTime", TimestampType()),
        ])

# read the stream
impressionsDf = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", "12342") \
        .load()
clicksDf = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", "12343") \
        .load()
# structure the data based on the schema defined - impressionDf
valueDF1 = impressionsDf.select(from_json(F.col("value"),impressionSchema).alias("value"))
impressionDfNew = valueDF1.select("value.*")
# structure the data based on the schema defined - clickDf
valueDF2 = clicksDf.select(from_json(F.col("value"),clickSchema).alias("value"))
clickDfNew = valueDF2.select("value.*")
#join condition
joinExpr = impressionDfNew["ImpressionID"] == clickDfNew["clickID"]
#join type
joinType="inner"
#joining both the streaming data frames
joinedDf = impressionDfNew.join(clickDfNew,joinExpr,joinType) \
.drop(clickDfNew["clickID"])
#output to the sink
campaignQuery = joinedDf.writeStream \
        .format("console") \
        .outputMode("append") \
        .option("checkpointLocation", "D://software_installation//Pycharm_files//Pyspark_data//trendytech//checkpoint") \
        .trigger(processingTime="15 second")\
        .start()
campaignQuery.awaitTermination()