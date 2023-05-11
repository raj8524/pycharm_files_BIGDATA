from pyspark.sql import SparkSession
from gkfunctions import read_schema
from pyspark.sql import functions as psf
from datetime import datetime, date, time, timedelta
import configparser
# Creating spark session
spark = SparkSession.builder.appName("DataIngestAndRefine").master("local").getOrCreate()

#Reding the data from configs
config = configparser.ConfigParser()
config.read(r'../projects-configs/config.ini')
inputLocations = config.get('paths', 'inputLocation')
outputLocation=config.get('paths','outputLocation')
landingFileSchemaFromconf=config.get('schema','landingFileSchema')
holdFileSchemaFromconf=config.get('schema','HoldFileSchema')

#Reading Schema
landingFileSchema=read_schema(landingFileSchemaFromconf)
holdFileSchema=read_schema(holdFileSchemaFromconf)
# Creating spark context (if required)
sc = spark.sparkContext

#Handling files with DATE
today=datetime.now()
yesterdayDate=today - timedelta(1)
# currentDaySuffix= "_" + today.strftime("%d%m%Y")
# previousDaySuffix="_" + yesterdayDate.strftime("%d%m%Y")
currentDaySuffix="_05062020"
previousDaySuffix="_04062020"

#Reading landing zone
# landingFileSchema=StructType([StructField('sale_id',StringType(),True),
#                               StructField('product_id',StringType(),True),
#                               StructField('Quantity_sold',IntegerType(),True),
#                               StructField('vendor_id',StringType(),True),
#                               StructField('Sale_date',TimestampType(),True),
#                               StructField('sale_amount',DoubleType(),True),
#                               StructField('sale_currency',StringType(),True)])

landingFileDF=spark.read.schema(landingFileSchema).option('delimiter','|').csv(inputLocations + "Sales_Landing\SalesDump" + currentDaySuffix)
landingFileDF.createOrReplaceTempView("landingFile_sql_DF")
#Reading Previous Hold Data
PreviousHoldDF=spark.read.schema(holdFileSchema).option('delimiter','|').option("header",True)\
    .csv(outputLocation + "Hold\HoldData" + previousDaySuffix)
PreviousHoldDF.createOrReplaceTempView("previousHold_sql_DF")

refreshedLandingData_DF=spark.sql("select a.Sale_ID,a.Product_ID, "
                                  "CASE "
                                  "WHEN(a.Quantity_Sold IS NULL) THEN b.Quantity_Sold "
                                  "ELSE a.Quantity_Sold "
                                  "END AS Quantity_Sold, "
                                  "CASE "
                                  "WHEN(a.Vendor_ID IS NULL) THEN b.Vendor_ID  "
                                  "ELSE a.Vendor_ID  "
                                  "END AS Vendor_ID , "
                                  "a.Sale_Date,a.Sale_Amount,a.Sale_Currency "
                                  "from landingFile_sql_DF a left outer join previousHold_sql_DF b ON a.Sale_ID=b.Sale_ID")
# refreshedLandingData_DF.show()

validLandingDF=refreshedLandingData_DF.filter(psf.col("Quantity_Sold").isNotNull() & psf.col("Vendor_id").isNotNull())
# validLandingDF.show()
validLandingDF.createOrReplaceTempView("validLanding_sql_DF")
#ReleaseHold  and unRelease Hold data
releaseFromHold_DF=spark.sql("select vd.Sale_ID "
                             "FROM validLanding_sql_DF vd INNER JOIN previousHold_sql_DF phd "
                             "ON vd.Sale_ID=phd.Sale_ID ")
releaseFromHold_DF.createOrReplaceTempView("releaseFromHold_sql_DF")

notReleaseFromHold=spark.sql("select * FROM previousHold_sql_DF "
                             "WHERE Sale_ID NOT IN (select Sale_ID FROM releaseFromHold_sql_DF)")
notReleaseFromHold.createOrReplaceTempView("notReleaseFromHold_sql_DF")
invalidLandingDF=refreshedLandingData_DF\
    .filter(psf.col("Quantity_Sold").isNull() | psf.col("Vendor_id").isNull())\
    .withColumn("Hold_Reason",psf
                .when(psf.col("Quantity_Sold").isNull(),"Qty sold missing")
                .otherwise(psf.when(psf.col("Vendor_ID").isNull(),"Vendor_ID missing")))\
    .union(notReleaseFromHold)
# invalidLandingDF.show()
#writing the valid,Hold data to different files
validLandingDF.write\
    .mode("overwrite").option("delimiter",'|').option("header",True).csv(outputLocation +"Valid\ValidData" +currentDaySuffix)
invalidLandingDF.write\
    .mode("overwrite").option("delimiter",'|').option("header",True).csv(outputLocation +"Hold\HoldData" +currentDaySuffix)
