from pyspark.sql import SparkSession
from pyspark.sql.functions import lit,col,when,explode,flatten,map_keys,map_values,create_map,expr,regexp_replace,array_contains,udf
from pyspark.sql.types import StructType,StructField,StringType,MapType,IntegerType,ArrayType,BooleanType
emp_data=[(25,"pooja","sanger",{"hair":"red","eye":"black"},"BI z999",7001,10000,"Bihar",101,["java","c++","py"]),
     (35,"ankita","raj",{"hair":"brown","eye":"brown"},"HP 140AC",8001,3000,"HP",201,[]),
     (45,"ravi","gulati",{"hair":"white","eye":"black"},"AP bc91",15001,7000,"AP",301,["java","c++","py"]),
     (55,"rahul","gulati",{"hair":"grey","eye":"black"},"HP 130ac",18001,3000,"HP",401,["java","c++","py"]),
     (60,"rohit","patel",{"hair":"black","eye":"grey"},"UP m0n0",20001,5000,"UP",501,["java","c","py"]),
     (65,"palavi","raj",{"hair":"white","eye":"red"},"AP ac91",10001,7000,"AP",601,["java","c++","ruby"]),
     (70,"srikant","sanger",{"hair":"grey","eye":"black"},"BI x999",7001,10000,"Bihar",701,["java","Go","py"]),
     (75,"deepak","gupta",{"hair":"brown","eye":"black"},"UP m1n1",9001,3000,"HP",801,["java","Go","py"]),
     (80,"dheeraj","gupta",{"hair":"red","eye":"white"},"BI y999",15001,10000,"Bihar",901,["java","c++","py"]),
     (85,"sandhya","sri",{"hair":"brown","grey":"black"},"UP s2t2",20001,5000,"UP",1001,["java","py"])]
emp_schema=StructType([StructField("emp_id",IntegerType(),True),StructField("fname",StringType(),True),StructField("lname",StringType(),True),
                 StructField("color",MapType(StringType(),StringType()),True),StructField("address",StringType(),True),
                 StructField("salary",IntegerType(),True),StructField("bonus",IntegerType(),True),
                 StructField("state",StringType(),True),StructField("dept_id",IntegerType(),True),
                      StructField("Lang",ArrayType(StringType()),True)])
spark=SparkSession.builder.appName("employee").getOrCreate()
emp_df=spark.createDataFrame(data=emp_data,schema=emp_schema)
emp_df_filter=emp_df.filter(col("lname")=='gupta')
emp_df_filter.explain()
#----------------------------M1---UDF------------------------------------------
def regenn(fname):
    if fname=="pooja":
        return 'P'
    elif fname=='ravi':
        return 'R'
    else :
        return fname

regeneratedName=udf(regenn,StringType())
# emp_df.withColumn("newfname",regeneratedName('fname')).show(truncate=0)

#-------------------------M2------------------------------UDF-------------------------------------------------------
emp_df.createOrReplaceTempView('test')
def regenn(fname):
    if fname=="pooja":
        return 'P'
    elif fname=='ravi':
        return 'R'
    else :
        return fname
spark.udf.register('newName',regenn)
spark.sql("select *,newName(fname) from test").show(truncate=0)

#--------------------------------filter for UDF---------------------
def regennFILTER(lname):
   return lname=='gupta'             #returning boolean

regeneratedFILTER=udf(regennFILTER,BooleanType())
# emp_df.withColumn("newfname",regeneratedName('fname')).show(truncate=0)
emp_df_udf=emp_df.filter(regeneratedFILTER('lname'))
emp_df_udf.explain()

#----we can use normal filter than udfFILTER.normal filter is more efficient and performance tuned.
