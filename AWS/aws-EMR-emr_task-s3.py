# https://aws-dojo.com/ws34/labs/run-task/

#spark-submit s3://emr-demo-2/script/aws-EMR-emr_task-s3.py s3://emr-demo-2/input/customers.csv s3://emr-demo-2/output      -> to run spark job on EMR from emr master
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark=SparkSession.builder.appName("fruits").getOrCreate()
# df=spark.read.option("inferschema","true").option("header","true").csv("s3://emr-demo-2/input/customers.csv")
df=spark.read.option("inferschema","true").option("header","true").csv(sys.argv[1])
df2=df.select("CUSTOMERNAME","EMAIL")
# df2.write.format("parquet").mode("overwrite").save("s3://emr-demo-2/aggregation/")
df2.write.format("csv").mode("overwrite").save(sys.argv[2])