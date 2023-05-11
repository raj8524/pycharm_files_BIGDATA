from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,DoubleType,TimestampType,IntegerType,FloatType
from pyspark.sql import functions as psf
from datetime import datetime,date,time,timedelta
from GkFunctions import read_Schema
import configparser
spark=SparkSession.builder.appName("VendorEnrichment").master("local").getOrCreate()

# Fetching config file
config=configparser.ConfigParser()
config.read(r'../projectconfigs/config.ini')
inputLocation=config.get('paths','inputLocation')
outputLocation=config.get('paths','outputLocation')
landingFileSchemaFromConf=config.get('schema','landingfileschema')

currentDayZoneSuffix="_05062020"
PreviousDayZoneSuffix="_04062020"

productPriceReferenceSchema=StructType([
    StructField('Product_ID',StringType(),True),
    StructField('Product_Name',StringType(),True),
    StructField('Product_Price',IntegerType(),True),
    StructField('Product_Price_Currency',StringType(),True),
    StructField('Sale_Amount',DoubleType(),True),
    StructField('Product_Updated_date',TimestampType(),True)
])

VendorReferenceSchema=StructType([
    StructField('Vendor_ID',StringType(),True),
    StructField('Vendor_Name',StringType(),True),
    StructField('Vendor_Add_Street',StringType(),True),
    StructField('Vendor_Add_City',StringType(),True),
    StructField('Vendor_Add_State',StringType(),True),
    StructField('Vendor_Add_Country',StringType(),True),
    StructField('Vendor_Add_Zip',StringType(),True),
    StructField('Vendor_Updated_Date',TimestampType(),True)
])

usdReferenceSchema= StructType([
    StructField('Currency',StringType(),True),
    StructField('Currency_Code',StringType(),True),
    StructField('Exchange_Rate',FloatType(),True),
    StructField('Currency_Updated_Date',TimestampType(),True)
])

productEnrichedDF=spark.read\
    .schema(productPriceReferenceSchema).option("delimiter","|").option("header",True)\
    .csv(outputLocation +"Enriched\SaleAmountEnrichment\SaleAmountEnrichment" +currentDayZoneSuffix)

productEnrichedDF.createOrReplaceTempView("productEnrichedDF")

VendorReferencedDF=spark.read\
    .schema(VendorReferenceSchema).option("delimiter","|").option("header",True)\
    .csv(inputLocation +"Vendors")
VendorReferencedDF.createOrReplaceTempView("VendorReferencedDF")

usdReferencedDF=spark.read\
    .schema(usdReferenceSchema).option("delimiter","|").option("header",True)\
    .csv(inputLocation +"USD_Rates")
usdReferencedDF.createOrReplaceTempView("usdReferencedDF")

vendorEnrichedDF=spark.sql("select a.*,b.Vendor_Name FROM "
                           "productEnrichedDF a INNER JOIN VendorReferencedDF b "
                           "ON a.Vendor_ID=b.Vendor_ID")
vendorEnrichedDF.createOrReplaceTempView("vendorEnrichedDF")

usdEnrichedDF=spark.sql("select * ,"
                        "ROUND((a.Sale_Amount/b.Exchange_Rate),2) as Amount_USD from "
                        "vendorEnrichedDF a JOIN usdReferencedDF b "
                        "ON a.Sale_Currency=b.Currency_code")
usdEnrichedDF.write.option("delimiter","|").option("header",True).mode("overwrite")\
    .csv(outputLocation + "Enriched/Vendor_USD_Enriched/Vendor_USD_Enriched" +currentDayZoneSuffix)

usdEnrichedDF.write.format('jdbc').options(
    url='jdbc:mysql://localhost:3306/gkstorespipelinedb',
    driver='com.mysql.jdbc.Driver',
    dbtable='finalsales',
    user='root',
    password='gkcodelabs'
).mode('append').save()