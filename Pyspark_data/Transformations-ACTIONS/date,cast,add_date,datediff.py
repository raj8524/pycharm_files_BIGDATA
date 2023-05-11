from pyspark.sql import SparkSession
from pyspark.sql.functions import date_add,to_date,col,expr
from pyspark.sql.types import *
spark=SparkSession.builder.master("local").appName("date").getOrCreate()
schema=StructType([StructField("Rechargeid",StringType(),True),StructField("Rechargedate",IntegerType(),True),
                   StructField("days_left",IntegerType(),True),StructField("paymentMode",StringType(),True)])
df_date=spark.read.option("header","true").option("schema","true").option("delimiter","true").csv("D:\Study\pyspark-docs/date,cast,date_add.txt")
# df_date.printSchema()
df_date.show()
#to_date require string column then it will convert to date

# df_date.select(to_date(col("Rechargedate").cast("string"),"yyyyMMDD")).show()
df_expr=df_date.withColumn("date_s",to_date(col("Rechargedate").cast("string"),"yyyyMMDD"))
df_expr.select("*",expr("date_add(date_s,days_left)")).show()

