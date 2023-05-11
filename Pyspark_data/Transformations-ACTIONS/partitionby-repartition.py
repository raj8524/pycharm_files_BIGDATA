from pyspark.sql import SparkSession
spark=SparkSession.builder.master("local").appName("keys").getOrCreate()

df=spark.read.option("delimiter",",").option("header",True).csv("D:\Study\pyspark-docs/transactions_data.txt")
# df.show()
# print(df.rdd.getNumPartitions())
# df.select("Customer_No").distinct().show()
# print(df.select("Customer_No").distinct().count())

#3 repartition will be created
# print(df.repartition(3).rdd.getNumPartitions())

#spark by default will created optimised number of repartitions on basis of customer_No
# print(df.repartition("Custpmer_No").rdd.getNumPartitions())

#3 repartion will be created on basis of customer_No
# print(df.repartition(3,"Custpmer_No").rdd.getNumPartitions())

#to check how many number of records r there in each partitions
from pyspark.sql.functions import spark_partition_id
df_count=df.repartition(3,"Customer_No").withColumn("partitionID",spark_partition_id()).groupby("partitionID").count()
# df_count.show()
# df.repartition(3,"Customer_No").withColumn("partitionID",spark_partition_id()).show()

#---------------------------------writing---------------
# df.write.partitionBy("Customer_No")  #partitioby is used while writing
# df.repartition("Customer_No").write.format("csv").option("header",True).mode("overwrite")\
#     .save("D:\Study\pyspark-docs/repartition_chk")
# df.write.partitionBy("Customer_No") .format('csv').save("D:\Study\pyspark-docs/partby_chk")

#repartition saves the data in spark memory where as partitionby writes directly in to disk.
#repartition involves shuffle where as partitionby doesnt involve shuffle
#partitionby will create files based on the partionby(column) i.e customer_No has 7 unique customers.so files r created in target.
#partitionby is improving the performance as data is segregated.

#--------------------reading partitionby data which will be faster-----------
df2=spark.read.option("header",True).csv("D:\Study\pyspark-docs/partby_chk").filter('Customer_No=1000210')
df2.show()