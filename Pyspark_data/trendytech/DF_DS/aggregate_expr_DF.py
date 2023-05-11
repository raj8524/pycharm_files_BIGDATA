from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
my_conf = SparkConf()
my_conf.set("spark.app.name", "my first application")
my_conf.set("spark.master","local[*]")

my_conf.set("spark.sql.adaptive.enabled","True")
my_conf.set("spark.sql.adaptive.skewJoin.enabled","True")
my_conf.set("spark.sql.adaptive.coalescePartitions.enabled","True")

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()
invoiceDF = spark.read\
    .format("csv")\
    .option("header",True)\
    .option("inferSchema",True) \
    .option("path","D://Study//TrendyTechInsight//week12spark//order_data.csv") \
    .load()
#column object expression
summaryDF = invoiceDF.groupBy("Country","InvoiceNo")\
    .agg(sum("Quantity").alias("TotalQuantity"),
    sum(expr("Quantity * UnitPrice")).alias("InvoiceValue"))
# summaryDF.show()
#string expression
summaryDf1 = invoiceDF.groupBy("Country","InvoiceNo")\
    .agg(expr("sum(Quantity) as TotalQunatity"),
    expr("sum(Quantity * UnitPrice) as InvoiceValue"))
# summaryDf1.show()
#spark SQL
invoiceDF.createOrReplaceTempView("sales")
spark.sql("""select country,InvoiceNo,sum(Quantity) as totQty,sum(Quantity * UnitPrice) as
InvoiceValue from sales group by country,InvoiceNo""").show()