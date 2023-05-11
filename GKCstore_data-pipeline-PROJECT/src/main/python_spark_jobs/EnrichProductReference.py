#its for calculating total sale amount once valid Datafile generated.

from pyspark.sql import SparkSession
from gkfunctions import read_schema
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType,DoubleType
from pyspark.sql import functions as psf
from datetime import datetime, date, time, timedelta
import configparser
# Creating spark session
spark = SparkSession.builder.appName("EnrichproductReference").master("local").getOrCreate()
config = configparser.ConfigParser()
config.read(r'../projects-configs/config.ini')
inputLocations = config.get('paths', 'inputLocation')
outputLocation=config.get('paths','outputLocation')
landingFileSchemaFromconf=config.get('schema','landingFileSchema')

currentDaySuffix="_05062020"
previousDaySuffix="_04062020"

productPriceReferenceSchema=StructType([StructField('Product_ID',StringType(),True),
                                        StructField('Product_Name',StringType(),True),
                                        StructField('Product_Price',IntegerType(),True),
                                        StructField('Product_Price_Currency',StringType(),True),
                                        StructField('Product_updated_date',TimestampType(),True)])


#Reading Schema for validfile
validFileSchema=read_schema(landingFileSchemaFromconf)

validDataDF=spark.read.schema(validFileSchema)\
    .option('delimiter','|').option("header",True)\
    .csv(outputLocation + "valid/ValidData" + currentDaySuffix)

validDataDF.createOrReplaceTempView("validDataDF_sql")
#Reading Project Reference
productPriceReferenceDF=spark.read.schema(productPriceReferenceSchema)\
    .option('delimiter','|').option("header",True)\
    .csv(inputLocations + "Products")
productPriceReferenceDF.createOrReplaceTempView("productPriceReferenceDF_sql")

productEnrichedDF=spark.sql("select a.Sale_ID,a.Product_ID,b.Product_Name, "
                            "a.Quantity_Sold,a.Vendor_ID,a.Sale_Date, "
                            "b.Product_Price * a.Quantity_Sold as Sale_Amount, "
                            "a.Sale_Currency "
                            " FROM validDataDF_sql a INNER JOIN productPriceReferenceDF_sql b "
                            "ON a.Product_ID=b.product_ID")

productEnrichedDF.write\
    .mode("overwrite").option("delimiter",'|').option("header",True)\
    .csv(outputLocation +"Enriched/SaleAmountEnrichment/SaleAmountEnrichment" +currentDaySuffix)















