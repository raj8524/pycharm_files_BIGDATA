#1.stream the live data and calculate the sales_amounts for live sales.
#2.Get the aggregated values of Total Sales and quatities of Products being sold by each vendor in streaming data.
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType,DoubleType
from pyspark.sql import functions as psf
from datetime import datetime, date, time, timedelta
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("DataIngestAndRefine").master("local").getOrCreate()
productInputLocation="D:\Study\GKcodelabs\PySpark-Updates\PySpark-Updates\GKCodelabs-PySpark-Realtime-Sales-Data-Analysis\GKCodelabs-PySpark-Realtime-Sales-Data-Analysis\Products_Reference"
streamInputLocation="D:\Study\GKcodelabs\PySpark-Updates\PySpark-Updates\GKCodelabs-PySpark-Realtime-Sales-Data-Analysis\GKCodelabs-PySpark-Realtime-Sales-Data-Analysis\Sales_Live"

# saleStreamSchema will be live sales data landing in streamInputLocation
saleStreamSchema=StructType([StructField('sale_id',StringType(),True),
                              StructField('product_id',StringType(),True),
                              StructField('Quantity_sold',IntegerType(),True),
                              StructField('vendor_id',StringType(),True),
                              StructField('Sale_date',TimestampType(),True),
                              StructField('sale_amount',DoubleType(),True),
                              StructField('sale_currency',StringType(),True)])

productPriceReferenceSchema=StructType([StructField('Product_ID',StringType(),True),
                                        StructField('Product_Name',StringType(),True),
                                        StructField('Product_Price',IntegerType(),True),
                                        StructField('Product_Price_Currency',StringType(),True),
                                        StructField('Product_updated_date',TimestampType(),True)])

productPriceReferenceDF=spark.read.schema(productPriceReferenceSchema)\
    .option('delimiter','|').option("header",True)\
    .csv(productInputLocation)
# productPriceReferenceDF.show()
productPriceReferenceDF.createOrReplaceTempView("productPriceReferenceDF_sql")

saleStreamDF=spark.readStream.schema(saleStreamSchema).option("delimiter","|").csv(streamInputLocation)
saleStreamDF.createOrReplaceTempView("saleStreamDF_sql")

#to read the streaming data on console.works same way as show()
# saleStreamDF.writeStream.format("console").outputMode("append").start().awaitTermination()

saleStreamDFWithAmount=spark.sql("select sd.Sale_ID,sd.Product_ID,sd.Quantity_Sold,sd.Vendor_ID,sd.Sale_Date, "
                                 "pr.Product_Price * sd.Quantity_Sold as sale_Amount, "
                                 "sd.Sale_Currency "
                                 "from saleStreamDF_sql sd left outer join "
                                 "productPriceReferenceDF_sql pr "
                                 "ON sd.Product_ID= pr.product_ID")

saleStreamDFWithAmount.createOrReplaceTempView("saleStreamDFWithAmount_sql")

saleStreamDFAggregated=spark.sql("select Product_ID,Vendor_ID,sum(Quantity_Sold) as Quantity_Sold,sum(Sale_Amount) as Sale_Amount "
                                 "FROM saleStreamDFWithAmount_sql "
                                 "GROUP BY Product_ID,Vendor_ID")

#spark optimisation by setting partition to 2
print(spark.conf.get("spark.sql.shuffle.partitions"))
spark.conf.set("spark.sql.shuffle.partitions", 2)
print(spark.conf.get("spark.sql.shuffle.partitions"))

saleStreamDFAggregated.writeStream.format("console").option("numRows",50).outputMode("complete").start().awaitTermination()
