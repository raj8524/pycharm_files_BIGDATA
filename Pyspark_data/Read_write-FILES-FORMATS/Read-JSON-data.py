from pyspark.sql import SparkSession
from pyspark.sql.functions import explode,col,json_tuple,from_json,to_json
spark=SparkSession.builder.master("local").appName("keys").getOrCreate()
df_json=spark.read.json("D:\Study\pyspark-docs/sample4.json",multiLine=True)
# df_json.show()
df2=df_json.withColumn("people_list",explode("people")).drop("people")
# df2.printSchema()
# df2.select("people_list.*").show()


df_multi_json=spark.read.option("header",True).option("escape",'\'').option('multiline','true').option("delimiter",'|').csv("D:\Study\pyspark-docs/dummy2.csv")
# df_multi_json.show(truncate=0)
#using JSON tuple

# df_multi_json.select("*",json_tuple("request","Response")).drop("request")\
#     .select("*",json_tuple("c0","MessageId","Latitude","longitude")).alias("MessageId","Latitude","longitude").drop("c0")

#from_json   .flatening column which nested json.--------------------------------------------------------
df_multi_json.select(col("request").alias("jsoncol"))
x=df_multi_json.select(col("request").alias("jsoncol")).rdd.map(lambda x:x.jsoncol).collect()
# print(x)

                               #----------------------------------------to get the schema
in_shema=spark.read.json(df_multi_json.select(col("request").alias("jsoncol")).rdd.map(lambda x:x.jsoncol)).schema
# print(in_shema)

                                 #--------------------------------now to get requset data with in_schema
json_data=df_multi_json.select("*",from_json("request",in_shema).alias("jsonstr"))
json_data.show(truncate=0)

col1=json_data.schema['jsonstr'].dataType.names[0]
chk="jsonstr."+col1+".*"
json_data.select("*",col(chk)).drop("request","jsonstr").show()


"""
if in json file format,prinschema is array.then use explode on that array column to make struct type.then further those struct type
can be made into independent columns.
"""






















