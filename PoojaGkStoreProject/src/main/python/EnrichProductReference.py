from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,DoubleType,TimestampType,IntegerType
from pyspark.sql import functions as psf
from datetime import datetime,date,time,timedelta
from GkFunctions import read_Schema
import configparser
spark=SparkSession.builder.appName("EnrichProductReference").master("local").getOrCreate()

# Fetching config file
config=configparser.ConfigParser()
config.read(r'../projectconfigs/config.ini')
inputLocation=config.get('paths','inputLocation')
outputLocation=config.get('paths','outputLocation')
landingFileSchemaFromConf=config.get('schema','landingfileschema')

productPriceReferenceSchema=StructType([
    StructField('Product_ID',StringType(),True),
    StructField('Product_Name',StringType(),True),
    StructField('Product_Price',IntegerType(),True),
    StructField('Product_Price_Currency',StringType(),True),
    StructField('Product_Updated_date',TimestampType(),True)
])


currentDayZoneSuffix="_05062020"
PreviousDayZoneSuffix="_04062020"

# Reading the schema
ValidFileSchema=read_Schema(landingFileSchemaFromConf)
ValidDataDF=spark.read.schema(ValidFileSchema).option("delimiter","|")\
    .csv(outputLocation + "Valid/ValidData" +currentDayZoneSuffix)

ValidDataDF.createOrReplaceTempView("ValidDataDF")

# Reading product reference
productPriceReferenceDF=spark.read.schema(productPriceReferenceSchema).option("delimiter","|").option("header",True)\
    .csv(inputLocation + "Products")
productPriceReferenceDF.createOrReplaceTempView("productPriceReferenceDF")

productEnrichedDf= spark.sql("select vd.Sale_Id,vd.Product_ID,ppf.Product_Name,vd.Quantity_Sold,vd.Vendor_Id,vd.Sale_Date "
                             ",ppf.Product_Price * vd.Quantity_Sold as Sale_Amount "
                             "from "
                             "ValidDataDF vd INNER JOIN productPriceReferenceDF ppf "
                             "ON vd.Product_ID=ppf.Product_ID ")
productEnrichedDf.write.option("header",True).option("delimiter","|").mode("overwrite")\
    .csv(outputLocation + "Enriched\SaleAmountEnrichment\SaleAmountEnrichment" +currentDayZoneSuffix)