from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
my_conf = SparkConf()
my_conf.set("spark.app.name", "my first application")
my_conf.set("spark.master","local[*]")
spark = SparkSession.builder.config(conf=my_conf).getOrCreate()
readFileDf = spark.read \
                .format("csv") \
                .option("path", "D://Study//Final_LP1.csv") \
                .option("header",True)\
                .load()
readFileDf.printSchema()

# 3,drop columns
dropFieldsDF=readFileDf.drop("Catalog","permissiongrantable")
# readFileDf.show()
# 4,9,12,5,13,14
new_columnsDF=dropFieldsDF.withColumn("Aws_Account_Id",lit("935284207569").cast("String"))\
                        .withColumn("Batch_Date",current_date().cast("date"))\
                        .withColumn("S3_Bucket_Nm",lit("null").cast("String"))\
                        .withColumn("S3_Prefix_Txt",lit("null").cast("String"))\
                        .withColumn("Zone_Nm",lit("us-east-1").cast("String"))\
                        .withColumn("Include_Column_List_Txt",when(col("Resource").contains("Included"),col("Resource")).otherwise(lit("null")).cast("String"))\
                        .withColumn("exclude_column_list_txt",when(col("Resource").contains("Excluded"),col("Resource")).otherwise(lit("null")).cast("String"))

# 6,7,8   column renamed
RenamedColumnDF=new_columnsDF.withColumnRenamed("Principal","Principal_nm")\
                             .withColumnRenamed("Table","Table_Nm") \
                             .withColumnRenamed("DatabaseName", "Database_Nm")\
                             .withColumnRenamed("permission", "Permission_Desc")\
                            .withColumnRenamed("Resource type", "Resource_Type_Desc")

# RenamedColumnDF.show(truncate=False)
RenamedColumnDF.createOrReplaceTempView("Final_LP")
finalDF=spark.sql("""select Aws_Account_Id,Batch_Date,Principal_Nm,Database_Nm,Table_Nm,S3_Bucket_Nm,S3_Prefix_Txt,Permission_Desc,
              Resource_Type_Desc,Zone_Nm,Include_Column_List_Txt,exclude_column_list_txt from Final_LP""").distinct()
# print(finalDF.count())
# finalDF.show()
finalDF.coalesce(1).write.mode("append").format("csv").save("D://Study//Study1_test_files")
