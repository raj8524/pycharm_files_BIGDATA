from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType,StructField,IntegerType,DoubleType,StringType,TimestampType
from GkFunctions import read_Schema
import configparser
from datetime import date,datetime,timedelta
from pyspark.sql import functions as psf

# Initiating Spark Session
spark= SparkSession.builder.appName("DataIngestAndRefine").master("local").getOrCreate()

# reading config
config= configparser.ConfigParser()
config.read(r'../projectconfigs/config.ini')
inputLocation=config.get('paths','inputLocation')
outputLocation=config.get('paths','outputLocation')

HoldFileSchemaFromConf=config.get('schema','Holdfileschema')
HoldFileSchema= read_Schema(HoldFileSchemaFromConf)

# Handling Date

today_date=datetime.now()
# print(today_date)
yesterday_date=today_date- timedelta(1)
# print(yesterday_date)
# date format  DDMMYYY
# previoudDaySuffix="_" + yesterday_date.strftime("%d%m%Y")
# CurrentDaySuffix="_" + today_date.strftime("%d%m%Y")
# print(previoudDaySuffix)
# print(CurrentDaySuffix)
CurrentDaySuffix="_05062020"
previoudDaySuffix="_04062020"

# Reading Landing Zone
# landingFileSchema= StructType([
#         StructField('Sale_Id',StringType(),True),
#         StructField('Product_Id',StringType(),True),
#         StructField('Quantity_Sold',IntegerType(),True),
#         StructField('Vendor_Id',StringType(),True),
#         StructField('Sale_Date',TimestampType(),True),
#         StructField('Sale_Amount',DoubleType(),True),
#         StructField('Sale_Currency',StringType(),True),
#     ])


LandingFileDF= spark.read.schema(HoldFileSchema).format("csv").option("delimiter","|").\
    option("path",inputLocation + "Sales_Landing/SalesDump" + CurrentDaySuffix).load()

LandingFileDF.createOrReplaceTempView("CurrentDayTable")

PreviousDayHoldDF= spark.read.schema(HoldFileSchema).format("csv").option("delimiter","|")\
    .option("path",outputLocation + "Hold\HoldData" + previoudDaySuffix).load()

PreviousDayHoldDF.createOrReplaceTempView("PreviousDayTable")

refreshedLandingData=spark.sql("select a.Sale_Id,a.Product_Id,"
                               "case when(a.Quantity_Sold IS NULL) THEN b.Quantity_Sold "
                               "ELSE a.Quantity_sold "
                               "END AS Quantity_Sold, "
                               "case when(a.Vendor_ID IS NULL) THEN b.Vendor_ID "
                               "ELSE a.Vendor_ID "
                               "END AS Vendor_ID, "
                               "a.Sale_Date, a.Sale_Amount,a.Sale_Currency "
                               "from CurrentDayTable a left outer join PreviousDayTable b "
                               "ON a.Sale_Id=b.Sale_Id")

    # option("path","D:/Study/GKcodelabs/GKCodelabs-BigData-Batch-Processing-Course/GKCodelabs-BigData-Batch-Processing-Course/Data/Inputs/Sales_Landing/SalesDump_04062020").load()

validLandingDf=refreshedLandingData.filter(psf.col("Quantity_Sold").isNotNull() & psf.col("Vendor_Id").isNotNull())
validLandingDf.createOrReplaceTempView("validLandingDf")

releasedFromHold=spark.sql("select vd.Sale_Id "
                           "FROM validLandingDF vd JOIN PreviousDayTable phd "
                           "ON vd.Sale_Id=phd.Sale_Id")
releasedFromHold.createOrReplaceTempView("releasedFromHold")
notreleasedFromHold=spark.sql("select * from "
                              "PreviousDayTable where Sale_Id NOT IN (select Sale_Id FROM releasedFromHold)")
notreleasedFromHold.createOrReplaceTempView("notreleasedFromHold")
invalidLandingDf=refreshedLandingData\
    .filter(psf.col("Quantity_Sold").isNull() |
            psf.col("Vendor_Id").isNull())\
    .withColumn("Hold_Reason",psf.when(psf.col("Quantity_Sold").isNull(),"Quantity sold is missing")
                .otherwise(psf.when(psf.col("Vendor_Id").isNull(),"Vendor Id is missing")))\
    .union(notreleasedFromHold)



validLandingDf.write\
    .mode("overwrite")\
    .option("delimiter","|")\
    .format("csv")\
    .option("path",outputLocation + "Valid\ValidData" +CurrentDaySuffix)\
    .save()

invalidLandingDf.write\
    .mode("overwrite")\
    .option("delimiter","|")\
    .format("csv")\
    .option("path",outputLocation + "Hold\HoldData" +CurrentDaySuffix)\
    .save()


# refreshedLandingData.show()