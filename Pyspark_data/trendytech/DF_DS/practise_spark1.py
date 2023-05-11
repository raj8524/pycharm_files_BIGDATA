from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
my_conf = SparkConf()
my_conf.set("spark.app.name", "my first application")
my_conf.set("spark.master","local[*]")
spark = SparkSession.builder.config(conf=my_conf).getOrCreate()
invoiceDF = spark.read\
    .format("csv")\
    .option("header",True)\
    .option("inferSchema",True) \
    .option("path","D://Study//TrendyTechInsight//week12spark//order_data.csv") \
    .load()

# invoiceDF.selectExpr("CustomerID","InvoiceNo","StockCode","Quantity*UnitPrice as price").show()

# invoiceDF.selectExpr("CustomerID","InvoiceNo","StockCode","Quantity*UnitPrice as price")\
#     .groupby("CustomerID").count().show()

# invoiceDF.selectExpr("CustomerID","InvoiceNo","coalesce(StockCode,-1)","Quantity*UnitPrice as price").show()

# invoiceDF.selectExpr("CustomerID","InvoiceNo","StockCode","Quantity*UnitPrice as price")\
#     .where(isnull("StockCode")).show()

# invoiceDF.selectExpr("CustomerID","InvoiceNo","StockCode","Quantity*UnitPrice as price")\
#     .groupby("CustomerID").count().alias('numberTimes')\
#     .withColumn("Categories",when(col("numberTimes")>20,"Gold")
#                             .when(col("numberTimes")>15,"silver")
#                             .when(col("numberTimes")>10,"platinum"))
#
# invoiceDF.selectExpr("CustomerID","InvoiceNo","StockCode","Quantity*UnitPrice as price")\
#     .withColumn("Categories",when(col("price")>20,"Gold")
#                             .when(col("price")>15,"silver")
#                             .when(col("price")>10,"platinum"))\
#                             .otherwise("bronze").show()

# invoiceDF.selectExpr("CustomerID","InvoiceNo","StockCode","Quantity*UnitPrice as price")\
#     .withColumn("Categories",expr("CASE when price>20 THEN 'Gold' "+
#                             "when price >15 THEN 'silver' "+
#                             "when price >10 THEN 'platinum' "+
#                             "ELSE 'bronze' END")).show()

# invoiceDF.selectExpr("CustomerID","InvoiceNo","StockCode","Quantity*UnitPrice as price")\
#     .select("CustomerID",expr("CASE when price>20 THEN 'Gold' "+
#                             "when price >15 THEN 'silver' "+
#                             "when price >10 THEN 'platinum' "+
#                             "ELSE 'bronze' END").alias("categories")).show()

# invoiceDF.createOrReplaceTempView("customers")
# spark.sql("""select CustomerID,Quantity*UnitPrice as price from customers""").select("CustomerID",
#             expr("CASE when price >20 THEN 'Gold' "
#             +"when price >15 THEN 'silver' "
#             +"when price >10 THEN 'platinum' "
#             +"ELSE 'bronze' END as categories" )).show()

# invoiceDF.createOrReplaceTempView("customers")
# spark.sql("select CustomerID,Quantity*UnitPrice as price,CASE when Quantity >20 THEN 'Gold' "
#             +"when Quantity >15 THEN 'silver' "
#             +"when Quantity >10 THEN 'platinum' "
#             +"ELSE 'bronze' END as categories from customers ").show()



# invoiceDF.createOrReplaceTempView("customers")
# spark.sql("""select CustomerID,Quantity*UnitPrice as price from customers where Quantity*UnitPrice IN
#               (select Quantity*UnitPrice from customers where Quantity*UnitPrice >15)""").show()

# invoiceDF.createOrReplaceTempView("customers")
# spark.sql("""select CustomerID,Quantity*UnitPrice as price from customers where CustomerID =
#               (select CustomerID from customers where CustomerID =16883 group by CustomerID)""").show()

# invoiceDF.createOrReplaceTempView("customers")
# spark.sql("""select CustomerID,Quantity*UnitPrice as price from customers a where  exists
#               (select CustomerID from customers b where a.CustomerID =b.CustomerID AND a.InvoiceDate=01-12-2010)""").show()

# invoiceDF.createOrReplaceTempView("customers")
# spark.sql("""select CustomerID,Quantity*UnitPrice as price from customers where CustomerID >
#               (select CustomerID from customers where CustomerID =16883 group by CustomerID)""").show()


# invoiceDF.drop_duplicates(["CustomerID","Description"]).select("CustomerID","Description")\
#     .filter(col("CustomerID").isNotNull()).show()
# invoiceDF.dropDuplicates(["CustomerID","Description"]).show()

#===================isnull,isNotNull in select statement going to True,false boolean.
# invoiceDF.drop_duplicates(["CustomerID","Description"])\
#                  .select(col("CustomerID").isNotNull(),"Description").show()
# invoiceDF.drop_duplicates(["CustomerID","Description"]).select(isnull("CustomerID"),"Description").show()


#
# invoiceDF.drop_duplicates(["CustomerID","Description"])\
#                  .select(col("Description").substr(7,7)).show(5)

# invoiceDF.drop_duplicates(["CustomerID","Description"])\
#                  .select("Description".strip("JUMBO")).filter(col("Description")=="JUMBO STORAGE BAG SUKI").show(10,truncate=False)
#
# invoiceDF.drop_duplicates(["CustomerID","Description"])\
#         .filter(col("Description")=="JUMBO STORAGE BAG SUKI").select("Description".lstrip("JUMBO")).show(10,truncate=False)


# invoiceDF.drop_duplicates(["CustomerID","Description"])\
#         .filter(col("Description")=="JUMBO STORAGE BAG SUKI").selectExpr("reverse(Description)").show(5,truncate=False)
#
# invoiceDF.drop_duplicates(["CustomerID","Description"])\
#         .filter(col("Description")=="JUMBO STORAGE BAG SUKI").selectExpr("Description".lstrip('JUMBO')).show(10,truncate=False)
#
# invoiceDF.drop_duplicates(["CustomerID","Description"])\
#                  .select("Description".strip("JUMBO")).filter(col("Description")=="JUMBO STORAGE BAG SUKI").show(10,truncate=False)

# invoiceDF.drop_duplicates(["CustomerID","Description"])\
#                  .select("Description".split(",")).filter(col("Description")=="JUMBO STORAGE BAG SUKI").show(10,truncate=False)

# invoiceDF.drop_duplicates(["CustomerID","Description"])\
#                  .select(split(col("Description)," ").getItem(0).alias('firstn'),
#                              "split(Description," ").getItem(1) as mid1",
#                              "split(Description," ").getItem(2) as mid2",
#                              "split(Description," ").getItem(3) as last")).filter(col("Description")=="JUMBO STORAGE BAG SUKI").show(10,truncate=False)



# applying split() using select()
# invoiceDF.select(
#                 split("InvoiceDate", '-').getItem(2).alias('yearTime'),
#                 split("InvoiceDate", '-').getItem(1).alias('month'),
#                 split("InvoiceDate", '-').getItem(0).alias('day')).show()

"""
regexp_replace() function is used to replace a string in a DataFrame column with another value,
translate() function to replace character by character of column values,
overlay() function to overlay string with another column string from start position and number of characters.

"""

invoiceDF.select(invoiceDF.columns[1:3]).show()
