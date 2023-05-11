from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark=SparkSession.builder.appName("tweets").getOrCreate()
df=spark.read.option("multiline","True").json("D:/tweets.json")
# df.show(truncate=False)
# df.printSchema()
# df.rdd.flatMap(lambda x:x[2].split(" ")).toDF().show()
df.select(split(col("text")," ").alias("text_el")).drop("text").show(truncate=False)
df1=df.withColumn("text_el",split(col("text")," ")).drop("text")
df1.show()
# starts_with_a = lambda s: s.startswith("#")
# df1.filter(exists(df1.text_el,starts_with_a)).show(truncate=False)
df1.selectExpr("id","explode(text_el) as textp").filter(col("textp").startswith("#")).groupby("id").agg(collect_list("textp")\
                                        .alias("texts")).show(truncate=False)


df1.selectExpr("id","explode(text_el) as textp").filter(col("textp").startswith("#")).groupby("id").count().show()
