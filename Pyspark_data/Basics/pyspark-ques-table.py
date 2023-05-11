from pyspark.sql import SparkSession
from pyspark.sql.functions import lit,col,when,explode,flatten,map_keys,map_values,create_map,expr,regexp_replace,array_contains

from pyspark.sql.types import StructType,StructField,StringType,MapType,IntegerType,ArrayType
emp_data=[(25,"pooja","sanger",{"hair":"red","eye":"black"},"BI z999",7001,10000,"Bihar",101,["java","c++","py"]),
     (35,"ankita","raj",{"hair":"brown","eye":"brown"},"HP 140AC",8001,3000,"HP",201,[]),
     (45,"ravi","gulati",{"hair":"white","eye":"black"},"AP bc91",15001,7000,"AP",301,["java","c++","py"]),
     (55,"rahul","gulati",{"hair":"grey","eye":"black"},"HP 130ac",18001,3000,"HP",401,["java","c++","py"]),
     (60,"rohit","patel",{"hair":"black","eye":"grey"},"UP m0n0",20001,5000,"UP",501,["java","c","py"]),
     (65,"palavi","raj",{"hair":"white","eye":"red"},"AP ac91",10001,7000,"AP",601,["java","c++","ruby"]),
     (70,"srikant","sanger",{"hair":"grey","eye":"black"},"BI x999",7001,10000,"null",701,["java","Go","py"]),
     (75,"deepak","gupta",{"hair":"brown","eye":"black"},"UP m1n1",9001,3000,"HP",801,["java","Go","py"]),
     (80,"dheeraj","gupta",{"hair":"red","eye":"white"},"BI y999",15001,10000,"Bihar",901,["java","c++","py"]),
     (85,"sandhya","sri",{"hair":"brown","grey":"black"},"UP s2t2",20001,5000,"UP",1001,"null")]
emp_schema=StructType([StructField("emp_id",IntegerType(),True),StructField("fname",StringType(),True),StructField("lname",StringType(),True),
                 StructField("color",MapType(StringType(),StringType()),True),StructField("address",StringType(),True),
                 StructField("salary",IntegerType(),True),StructField("bonus",IntegerType(),True),
                 StructField("state",StringType(),True),StructField("dept_id",IntegerType(),True),
                      StructField("Lang",ArrayType(StringType()),True)])
spark=SparkSession.builder.appName("employee").getOrCreate()
emp_df=spark.createDataFrame(data=emp_data,schema=emp_schema)
emp_df.show()
emp_df.select(emp_df.columns[2:5]).show()

dept_data=[(101,300000,"Daniel","hiring","","","HR"),(201,400000,"Rushendra","web_portal","03-01-2021 13:12:36","1st","finance"),
          (301,200000,"pushpa","","","","legal"),(401,500000,"ashok","network","04-01-2021 20:22:00","2nd","IT"),
          (501,1000000,"Swetank","App","05-01-2021","3rd","developer"),(601,400000,"Rushendra","web_portal","03-01-2021 15:20:39","1st","finance"),
          (701,500000,"Akshay","server","04-01-2021","2nd","IT"),(801,1000000,"Swetank","App","05-01-2021 19:07:07","3rd","developer")]
dept_schema=StructType([StructField("dept_id",IntegerType(),True),StructField("Budjet",IntegerType(),True),
                        StructField("manager",StringType(),True),StructField("product",StringType(),True),
                        StructField("submition-date",StringType(),True),StructField("winner",StringType(),True),
                        StructField("department",StringType(),True)])


dept_df=spark.createDataFrame(data=dept_data,schema=dept_schema)
# dept_df.show()
df3=emp_df.rdd.map(lambda x:(x.color["hair"],x.color["eye"])).toDF(['hairs','eyes'])
df3.show()

#1 . write different ways to convert color column element in 2 different column.
#---------------on dict column element, map,explode,map_keys,map_values be used . flatten is used on array of array i.e list------------------------------

# dept_df.show()
# emp_df.withColumn("hair",emp_df.color.hair)\
#        .withColumn("eye",emp_df.color.eye).show()
# emp_df.select(emp_df.color.hair.alias('hair'),emp_df.color.eye.alias("eye")).show()
# emp_df.select(col("color.hair").alias('hair'),col("color.eye").alias("eye")).show()
# emp_df.rdd.map(lambda x:(x.color["hair"],x.color["eye"]))\
#           .toDF(['hair','eye']).show()
# emp_df.withColumn("hair",emp_df.color.getItem('hair'))\
#        .withColumn("eye",emp_df.color.getItem('eye')).show()
# emp_df.withColumn("hair",emp_df.color.getField('hair'))\
#        .withColumn("eye",emp_df.color.getField('eye')).show()
# emp_df.select("fname",explode("color")).show()
emp_df.select(map_keys("color")).show()
emp_df.select(map_values("color")).show()

#2.write different ways to combine fname,lname as 1 column as fullname.---------------------------------------------------
#create_map gives dataset as dict in column whereas array gives list of dataset

# df4=emp_df.select(create_map(col("fname"),col("lname")).alias("Fullname"))
# df4.select(map_values(col("Fullname"))).show()
# emp_df.withColumn("Fullname",expr("fname ||' '||lname")).show()
# emp_df.select(expr("fname ||' '||lname").alias("fullname")).show()
# emp_df.select(array(col("fname"),col("lname")).alias("fullname")).show()


#3. write different ways to replaCE bihar ->bengal and HP ->haryana in state column.

df4 = emp_df.withColumn("new_state", when(col("state") == "Bihar", "Bengal") \
                        .when(col("state") == "HP", "Haryana") \
                        .otherwise(col("state"))).show()

emp_df.select(expr("CASE WHEN state ='Bihar' THEN 'Bengal'" + "WHEN state ='HP' THEN 'Haryana'"
                   + "ELSE state END").alias("new_state")).show()
emp_df.withColumn("new_state", expr("CASE WHEN state ='Bihar' THEN 'Bengal'" + "WHEN state ='HP' THEN 'Haryana'"
                                    + "ELSE state END")).show()
emp_df.createOrReplaceTempView("EMP")
spark.sql("select CASE WHEN state ='Bihar' THEN 'Bengal'" + "WHEN state ='HP' THEN 'Haryana'"
          + "ELSE state END as new_state from EMP").show()

#4. write different ways to filter column on matching condition of column data.
#----array_contains works on list, array is to store 2 columns in 1  ,   ArrayType is used in schema to make list of elements.

emp_df.filter(col("state"),contains("ha")).show()
emp_df.filter(array_contains(emp_df.Lang,"c++")).show()
li=["BI","HP"]
emp_df.filter(col('state').isin(li)).show()
emp_df.filter(col('state').like('%ha%')).show()

emp_df.createOrReplaceTempView("TAB")
spark.sql("select * from TAB where state like '%mih%'").show()

#4.-----------------regex_replace-------
emp_df.select(regexp_replace("address", "state", "match")).show()
emp_df.withColumn("new_state", regexp_replace("address", "BI", "bhutan")).show()
emp_df.withColumn("new_address",
                  when(col("address").startswith("BI"), regexp_replace("address", "BI", "Bhutan")) \
                  .when(col("address").startswith("AP"), regexp_replace("address", "bc", "black")) \
                  .when(col("address").endswith("m1n1"), regexp_replace("address", "m1n1", "utranchal")) \
                  .otherwise(emp_df.address)).show()

emp_df.select(translate("state", 'ahp', 'wuv').alias("changed_state")).show()    # ahp has to be consecutive,then only wuv will be done 1 by 1
emp_df.select(overlay(col("state"),col("address"),3).alias("crisp")).show()       #   address character will be replacd in state from length 3.
emp_df.select(countDistinct("salary","bonus")).show()

#..write code to groupby by state and find their salaries.


#..window func
from pyspark.sql.window import Window
windowSpec  = Window.partitionBy("manager").orderBy("product")
dept_df.withColumn("rank",rank().over(windowSpec)).show(truncate=False)
dept_df.withColumn("row_num",row_number().over(windowSpec)).show(truncate=False)


#-----------------group by
# emp_df.select(translate("state",'ahp','wuv').alias("changed_state")).show()
# emp_df.select(overlay(col("state"),col("address"),3).alias("crisp")).show()
# emp_df.select(countDistinct("salary","bonus")).show()
# emp_df.groupBy(col("state")).sum("salary").show()
# emp_df.groupBy(col("state")).max("salary").show()
# emp_df.groupBy(col("state"),col("Lang")).count().show()
# emp_df.groupBy(col("state")).sum("salary","bonus").orderBy("state").show()
# emp_df.groupBy(col("state")).sum("salary","bonus").orderBy(emp_df["state"].desc()).show()






