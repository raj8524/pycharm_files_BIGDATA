from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('Basics').getOrCreate()
df=spark.read.format("avro").load("Files/tables/file.avro")

#reading from folder
df1=spark.read.format("avro").load("Files/tables/")
d2=spark.read.format("avro").schema('schema').load("Files/tables/file.avro")
df.write.mode("overwrite").format('avro').save('Filestore/output')


df3=spark.read.format("parquet").load("Files/tables/file.avro")

#reading from folder
df4=spark.read.format("parquet").load("Files/tables/")
d5=spark.read.format("parquet").schema('schema').load("Files/tables/file.parquet")
df.write.mode("overwrite").format('parquet').save('Filestore/output')
