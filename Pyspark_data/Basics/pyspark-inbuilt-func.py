#-------------------------------- PANDAS TO PYSPARK-------------------
import pandas as pd
data=pd.read_json("D:\Data.json")
print(data)
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("pandas").getOrCreate()
df=spark.createDataFrame(data)
df.show()

#----------------------when --otherwise--------case--------expr---
from pyspark.sql import SparkSession
from pyspark.sql.functions import when,col,expr,add_months,substring
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
"""
data = [("James","M",60000),("Michael","M",70000),
        ("Robert",None,400000),("Maria","F",500000),
        ("Jen","",None)]

columns = ["name","gender","salary"]
df = spark.createDataFrame(data = data, schema = columns)
df.show()

df2 = df.withColumn("new_gender", when(df.gender == "M","Male")
                                 .when(df.gender == "F","Female")
                                 .when(df.gender.isNull() ,"")
                                 .otherwise(df.gender))

df3=df.select(col("*"),when(df.gender == "M","Male")
                  .when(df.gender == "F","Female")
                  .when(df.gender.isNull() ,"")
                  .otherwise(df.gender).alias("new_gender"))

df4 = df.withColumn("new_gender", expr("CASE WHEN gender = 'M' THEN 'Male' " +
               "WHEN gender = 'F' THEN 'Female'" +" WHEN gender IS NULL THEN ''" +
               "ELSE gender END"))

df5 = df.select(col("*"), expr("CASE WHEN gender = 'M' THEN 'Male' " +
           "WHEN gender = 'F' THEN 'Female'"+ "WHEN gender IS NULL THEN ''" +
           "ELSE gender END").alias("new_gender"))

df.createOrReplaceTempView("EMP")
spark.sql("select name, CASE WHEN gender = 'M' THEN 'Male' " +
               "WHEN gender = 'F' THEN 'Female' WHEN gender IS NULL THEN ''" +
              "ELSE gender END as new_gender from EMP").show()

df.createOrReplaceTempView("EMP")
spark.sql("select name,gender CASE WHEN gender='M' THEN 'MALE' " + "WHEN  gender ='F' THEN 'FEMALE' "+
          "WHEN gender = 'NULL' THEN ''" + "ELSE gender END as new_gend FROM EMP").show()

df.withColumn("new_column", when(col("code") == "a" | col("code") == "d", "A")
      .when(col("code") == "b" & col("amt") == "4", "B")
      .otherwise("A1")).show()


#-------------------------------------------expr---------------------------------------

# to concenate
df.withColumn("mix",expr("name ||','|| gender || '_' || salary")).show()

#---to add column value to another column

data=[("2019-01-23",1),("2019-06-24",2),("2019-09-20",3)]
df=spark.createDataFrame(data).toDF("date","increment")
df.select(df.date,df.increment,expr("add_months(date,increment)").alias("new_date")).show()

df.select(df.date,df.increment, expr("add_months(date,increment) as inc_date")
  ).show()

df.select(df.date,df.increment,
     expr("increment + 5 as new_increment")
  ).show()

df.filter(expr("date == increment")).show()
"""

#----------------------------------------Lit, typedLIT()---------------------

data = [("111",50000),("222",60000),("333",40000)]
columns= ["EmpId","Salary"]
df = spark.createDataFrame(data = data, schema = columns)
df.printSchema()
df.show(truncate=False)

from pyspark.sql.functions import col,lit
df2 = df.select(col("EmpId"),col("Salary"),lit("1").alias("lit_value1"))
df2.show(truncate=False)
from pyspark.sql.functions import when
df3 = df2.withColumn("lit_value2", when(col("Salary") >=40000 & col("Salary") <= 50000,lit("100")).otherwise(lit("200")))
df3.show(truncate=False)

#--------------------spilt----------------------------------------------
from pyspark.sql import SparkSession
spark = SparkSession.builder \
         .appName('SparkByExamples.com') \
         .getOrCreate()

data = [("James, A, Smith","2018","M",3000),
            ("Michael, Rose, Jones","2010","M",4000),
            ("Robert,K,Williams","2010","M",4000),
            ("Maria,Anne,Jones","2005","F",4000),
            ("Jen,Mary,Brown","2010","",-1)
            ]

columns=["name","dob_year","gender","salary"]
df=spark.createDataFrame(data,columns)


from pyspark.sql.functions import split, col
df2 = df.select(split(col("name"),",").alias("NameArray")) \
    .drop("name")
df2.show()

df.createOrReplaceTempView("PERSON")
spark.sql("select SPLIT(name,',') as NameArray from PERSON") \
    .show()

#----------------------------------------------------------substring,substr------------------------
df3=df.withColumn('year', col('date').substr(1, 4))\
  .withColumn('month',col('date').substr(5, 2))\
  .withColumn('day', col('date').substr(7, 2))

df3.show()
df.withColumn("year",substring('date',1,4))\
   .withColumn("month",substring('date',5,2))\
   .withColumn("day",substring('date',7,2)).show()
df.select("id",'date',substring('date',1,4).alias("year"),
          substring('date',5,2).alias("month"),
          substring('date',7,2).alias("day")).show()
df.select('date', substring('date', 1,4).alias('year'), \
                  substring('date', 5,2).alias('month'), \
                  substring('date', 7,2).alias('day')).show()

df.selectExpr('date', 'substring(date, 1,4) as year', \
                  'substring(date, 5,2) as month', \
                  'substring(date, 7,2) as day').show()


#-----------------------------------concat_ws-----------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,split,concat_ws
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
columns = ["name","languagesAtSchool","currentState"]
data = [("James,,Smith",["Java","Scala","C++"],"CA"), \
    ("Michael,Rose,",["Spark","Java","C++"],"NJ"), \
    ("Robert,,Williams",["CSharp","VB"],"NV")]
df = spark.createDataFrame(data=data,schema=columns)
# df.show()
df.select(concat_ws(',',col("languagesAtSchool")).alias("new_lang"),df.currentState).show()

df.createOrReplaceTempView("ARRAY_STRING")
spark.sql("select name, concat_ws(',',languagesAtSchool) as languagesAtSchool from ARRAY_STRING").show(truncate=False)

#-----------------------------------------------regexp_replace----------------------------
#-to replace part of string with another string,to replace string from dictionary element, to replace character of string with translate,1 column element with another column element
#to replace part of column matching another value column from particular value position

from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace,when,translate,expr,overlay
spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()
address = [(1,"14851 Jeffrey Rd","DE"),
    (2,"43421 Margarita St","NY"),
    (3,"13111 Siemon Ave","CA")]
df =spark.createDataFrame(address,["id","address","state"])
df.show()
df.withColumn("address",regexp_replace("address",'Rd','Road')).show()

df.withColumn('address',
    when(df.address.endswith('Rd'),regexp_replace(df.address,'Rd','Road')) \
   .when(df.address.endswith('St'),regexp_replace(df.address,'St','Street')) \
   .when(df.address.endswith('Ave'),regexp_replace(df.address,'Ave','Avenue')) \
   .otherwise(df.address)) \
   .show(truncate=False)
stateDic={'CA':'California','NY':'New York','DE':'Delaware'}
df2=df.rdd.map(lambda x:
               (x.id,x.address,stateDic[x.state])
              ).toDF(["id","address","state"])
df2.show()

df.withColumn('address', translate('address', '123', 'ABC')) \
  .show(truncate=False)

df = spark.createDataFrame(
   [("ABCDE_XYZ", "XYZ","FGH")],
    ["col1", "col2","col3"]
  )
df.withColumn("new_column",
              expr("regexp_replace(col1, col2, col3)")
              .alias("replaced_value")
              ).show()

df = spark.createDataFrame([("ABCDE_XYZ", "FGH")], ("col1", "col2"))
df.select(overlay("col1", "col2", 7).alias("overlayed")).show()

#---------------------------------------timestamp---------------------------------
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
    .appName('SparkByExamples.com') \
    .getOrCreate()

from pyspark.sql.functions import *

df = spark.createDataFrame(
    data=[("1", "2019-06-24 12:01:19.000")],
    schema=["id", "input_timestamp"])
df.printSchema()

# Timestamp String to DateType
df.withColumn("timestamp", to_timestamp("input_timestamp")) \
    .show(truncate=False)

# Using Cast to convert TimestampType to DateType
df.withColumn('timestamp', \
              to_timestamp('input_timestamp').cast('string')) \
    .show(truncate=False)

df.select(to_timestamp(lit('06-24-2019 12:01:19.000'), 'MM-dd-yyyy HH:mm:ss.SSSS')) \
    .show(truncate=False)

# SQL string to TimestampType
spark.sql("select to_timestamp('2019-06-24 12:01:19.000') as timestamp")
# SQL CAST timestamp string to TimestampType
spark.sql("select timestamp('2019-06-24 12:01:19.000') as timestamp")
# SQL Custom string to TimestampType
spark.sql("select to_timestamp('06-24-2019 12:01:19.000','MM-dd-yyyy HH:mm:")

#----------------------------------------to_date       --------------------------------------------
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
    .appName('SparkByExamples.com') \
    .getOrCreate()

df = spark.createDataFrame(
    data=[("1", "2019-06-24 12:01:19.000")],
    schema=["id", "input_timestamp"])
df.printSchema()

from pyspark.sql.functions import *

# Using Cast to convert Timestamp String to DateType
df.withColumn('date_type', col('input_timestamp').cast('date')) \
    .show(truncate=False)

# Using Cast to convert TimestampType to DateType
df.withColumn('date_type', to_timestamp('input_timestamp').cast('date')) \
    .show(truncate=False)

df.select(to_date(lit('06-24-2019 12:01:19.000'), 'MM-dd-yyyy HH:mm:ss.SSSS')) \
    .show()

# Timestamp String to DateType
df.withColumn("date_type", to_date("input_timestamp")) \
    .show(truncate=False)

# Timestamp Type to DateType
df.withColumn("date_type", to_date(current_timestamp())) \
    .show(truncate=False)

df.withColumn("ts", to_timestamp(col("input_timestamp"))) \
    .withColumn("datetype", to_date(col("ts"))) \
    .show(truncate=False)

# SQL TimestampType to DateType
spark.sql("select to_date(current_timestamp) as date_type")
# SQL CAST TimestampType to DateType
spark.sql("select date(to_timestamp('2019-06-24 12:01:19.000')) as date_type")
# SQL CAST timestamp string to DateType
spark.sql("select date('2019-06-24 12:01:19.000') as date_type")
# SQL Timestamp String (default format) to DateType
spark.sql("select to_date('2019-06-24 12:01:19.000') as date_type")
# SQL Custom Timeformat to DateType
spark.sql("select to_date('06-24-2019 12:01:19.000','MM-dd-yyyy HH:mm:ss.SSSS') as date_type")

#--------------explode ---flatten--------array(array(stringtype))-----------------------------------------------
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('pyspark-by-examples').getOrCreate()

arrayArrayData = [
  ("James",[["Java","Scala","C++"],["Spark","Java"]]),
  ("Michael",[["Spark","Java","C++"],["Spark","Java"]]),
  ("Robert",[["CSharp","VB"],["Spark","Python"]])
]

df = spark.createDataFrame(data=arrayArrayData, schema = ['name','subjects'])
df.printSchema()
df.show(truncate=False)

from pyspark.sql.functions import explode
df.select(df.name,explode(df.subjects)).show(truncate=False)

from pyspark.sql.functions import flatten
df.select(df.name,flatten(df.subjects)).show(truncate=False)


#---------------------------------------------------ArrayType-----------------------------------------
from pyspark.sql.types import StringType, ArrayType
arrayCol = ArrayType(StringType(),False)

data = [
 ("James,,Smith",["Java","Scala","C++"],["Spark","Java"],"OH","CA"),
 ("Michael,Rose,",["Spark","Java","C++"],["Spark","Java"],"NY","NJ"),
 ("Robert,,Williams",["CSharp","VB"],["Spark","Python"],"UT","NV")
]

from pyspark.sql.types import StringType, ArrayType,StructType,StructField
schema = StructType([
    StructField("name",StringType(),True),
    StructField("languagesAtSchool",ArrayType(StringType()),True),
    StructField("languagesAtWork",ArrayType(StringType()),True),
    StructField("currentState", StringType(), True),
    StructField("previousState", StringType(), True)
  ])

df = spark.createDataFrame(data=data,schema=schema)
df.printSchema()
df.show()
from pyspark.sql.functions import explode
df.select(df.name,explode(df.languagesAtSchool)).show()

from pyspark.sql.functions import split
df.select(split(df.name,",").alias("nameAsArray")).show()

from pyspark.sql.functions import array
df.select(df.name,array(df.currentState,df.previousState).alias("States")).show()

from pyspark.sql.functions import array_contains
df.select(df.name,array_contains(df.languagesAtSchool,"Java")
    .alias("array_contains")).show()

#-----------------------------------------------Aggregate func--------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName('pyspark-by-examples').getOrCreate()

simpleData = [("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4200),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)
  ]
schema = ["employee_name", "department", "salary"]
df = spark.createDataFrame(data=simpleData, schema = schema)
# df.show(truncate=False)
# print("approx_count_distinct: " + \
#       str(df.select(approx_count_distinct("salary")).collect()[0]))
print(df.select(approx_count_distinct("salary")).collect()[0][0])

# df.select(collect_list("salary")).show(truncate=False)
# df.select(collect_set("salary")).show(truncate=False)
# df2 = df.select(countDistinct("department", "salary"))
# df2.show(truncate=False)
df.select(sumDistinct("salary")).show(truncate=False)
print("count: "+str(df.select(count("salary")).collect()[0]))

#----------create_map--is used to convert selected DataFrame columns to MapType, create_map() takes a list of columns you wanted to convert as an argument and returns a MapType column.

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
data = [ ("36636","Finance",3000,"USA"),
    ("40288","Finance",5000,"IND"),
    ("42114","Sales",3900,"USA"),
    ("39192","Marketing",2500,"CAN"),
    ("34534","Sales",6500,"USA") ]
schema = StructType([
     StructField('id', StringType(), True),
     StructField('dept', StringType(), True),
     StructField('salary', IntegerType(), True),
     StructField('location', StringType(), True)
     ])
df = spark.createDataFrame(data=data,schema=schema)
df.show(truncate=False)
df.withColumn("properties",create_map(lit("salary"),col("salary"),lit("location"),col("location"))).drop("salary","location").show()

#-------------------------map-type------------------------------------
#MapType (also called map type) is a data type to represent Python Dictionary (dict) to store key-value pair, a MapType object comprises three fields, keyType (a DataType), valueType (a DataType) and valueContainsNull (a BooleanType)
#create_map(),MapType is used to form dictionary element in the column.map_keys ,map_values will give dict elements in list in 1 column.


from pyspark.sql.types import StructField, StructType, StringType, MapType
schema = StructType([
    StructField('name', StringType(), True),
    StructField('properties', MapType(StringType(),StringType()),True)
])
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
dataDictionary = [
        ('James',{'hair':'black','eye':'brown'}),
        ('Michael',{'hair':'brown','eye':None}),
        ('Robert',{'hair':'red','eye':'black'}),
        ('Washington',{'hair':'grey','eye':'grey'}),
        ('Jefferson',{'hair':'brown','eye':''})
        ]
df = spark.createDataFrame(data=dataDictionary, schema = schema)
df.show(truncate=False)
df3=df.rdd.map(lambda x: \
    (x.name,x.properties["hair"],x.properties["eye"])) \
    .toDF(["name","hair","eye"])
# df3.show()
# df.withColumn("hair",df.properties.getItem("hair")) \
#   .withColumn("eye",df.properties.getItem("eye")) \
#   .drop("properties") \
#   .show()

# df.withColumn("hair",df.properties["hair"]) \
#   .withColumn("eye",df.properties["eye"]) \
#   .drop("properties") \
#   .show()
# from pyspark.sql.functions import explode
# df.select(df.name,explode(df.properties)).show()

from pyspark.sql.functions import map_keys
df.select(df.name,map_keys(df.properties)).show()

from pyspark.sql.functions import map_values
df.select(df.name,map_values(df.properties)).show()

#-------------------------------------------struct--------------------------------

structureData = [
    (("James","","Smith"),"36636","M",3100),
    (("Michael","Rose",""),"40288","M",4300),
    (("Robert","","Williams"),"42114","M",1400),
    (("Maria","Anne","Jones"),"39192","F",5500),
    (("Jen","Mary","Brown"),"","F",-1)
  ]
structureSchema = StructType([
        StructField('name', StructType([
             StructField('firstname', StringType(), True),
             StructField('middlename', StringType(), True),
             StructField('lastname', StringType(), True)
             ])),
         StructField('id', StringType(), True),
         StructField('gender', StringType(), True),
         StructField('salary', IntegerType(), True)
         ])

df2 = spark.createDataFrame(data=structureData,schema=structureSchema)
df2.printSchema()
df2.show(truncate=False)

from pyspark.sql.functions import col,struct,when
updatedDF = df2.withColumn("OtherInfo",
    struct(col("id").alias("identifier"),
    col("gender").alias("gender"),
    col("salary").alias("salary"),
    when(col("salary").cast(IntegerType()) < 2000,"Low")
      .when(col("salary").cast(IntegerType()) < 4000,"Medium")
      .otherwise("High").alias("Salary_Grade")
  )).drop("id","gender","salary")

updatedDF.printSchema()
updatedDF.show(truncate=False)

#------------------------------------------------------------------------distinctCount()-----------
from pyspark.sql import SparkSession
spark = SparkSession.builder \
         .appName('SparkByExamples.com') \
         .getOrCreate()

data = [("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)
  ]
columns = ["Name","Dept","Salary"]
df = spark.createDataFrame(data=data,schema=columns)
df.show()

print("Distinct Count: " + str(df.distinct().count()))

from pyspark.sql.functions import countDistinct
df2=df.select(countDistinct("department", "salary"))

df.createOrReplaceTempView("EMP")
spark.sql("select distinct(count(*)) from EMP").show()

#--------------------------Window func ---------------ROW,Row number------------------
#PySpark Window functions are used to calculate results such as the rank, row number e.t.c over a range of input rows.
#PySpark Window functions operate on a group of rows (like frame, partition) and return a single value for every input row.
#To perform an operation on a group first, we need to partition the data using Window.partitionBy() , and for row number and rank function we need to additionally order by on partition data using orderBy clause.

# row_number(): Column	Returns a sequential number starting from 1 within a window partition
# rank(): Column	Returns the rank of rows within a window partition, with gaps.rank() window function is used to provide a rank to the result within a window partition. This function leaves gaps in rank when there are ties.
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
simpleData = (("James", "Sales", 3000), \
              ("Michael", "Sales", 4600), \
              ("Robert", "Sales", 4100), \
              ("Maria", "Finance", 3000), \
              ("James", "Sales", 3000), \
              ("Scott", "Finance", 3300), \
              ("Jen", "Finance", 3900), \
              ("Jeff", "Marketing", 3000), \
              ("Kumar", "Marketing", 2000), \
              ("Saif", "Sales", 4100) \
              )

columns = ["employee_name", "department", "salary"]
df = spark.createDataFrame(data=simpleData, schema=columns)
# df.printSchema()
# df.show(truncate=False)
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank, percent_rank

windowSpec = Window.partitionBy("department").orderBy("salary")

df.withColumn("row_number",row_number().over(windowSpec)) \
    .show(truncate=False)
df.withColumn("rank",rank().over(windowSpec)) \
    .show()
df.withColumn("dense_rank",dense_rank().over(windowSpec)) \
    .show()
df.withColumn("percent_rank",percent_rank().over(windowSpec)) \
    .show()

#--------------------------------------------------lit() and typedLit()------------------------------
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
data = [("111",50000),("222",60000),("333",40000)]
columns= ["EmpId","Salary"]
df = spark.createDataFrame(data = data, schema = columns)
# df.printSchema()
# df.show(truncate=False)

from pyspark.sql.functions import col,lit,when
# df2 = df.select(col("EmpId"),col("Salary"),lit("1").alias("lit_value1"))
# # df2.show(truncate=False)

df3=df.withColumn("lit_value2", when(col("Salary") >=40000 & col("Salary") <= 50000,lit("100")).otherwise(lit("200")))
df3.show(truncate=False)

#--------------------------JSON-------------------------------------------------------------------
from pyspark.sql import SparkSession,Row
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

jsonString="""{"Zipcode":704,"ZipCodeType":"STANDARD","City":"PARC PARQUE","State":"PR"}"""
df=spark.createDataFrame([(1, jsonString)],["id","value"])
df.show(truncate=False)

#Convert JSON string column to Map type
from pyspark.sql.types import MapType,StringType
from pyspark.sql.functions import from_json
df2=df.withColumn("value",from_json(df.value,MapType(StringType(),StringType())))
df2.printSchema()
df2.show(truncate=False)

from pyspark.sql.functions import to_json,col
df2.withColumn("value",to_json(col("value"))) \
   .show(truncate=False)

from pyspark.sql.functions import json_tuple
df.select(col("id"),json_tuple(col("value"),"Zipcode","ZipCodeType","City")) \
    .toDF("id","Zipcode","ZipCodeType","City") \
    .show(truncate=False)

from pyspark.sql.functions import get_json_object
df.select(col("id"),get_json_object(col("value"),"$.ZipCodeType").alias("ZipCodeType")) \
    .show(truncate=False)




