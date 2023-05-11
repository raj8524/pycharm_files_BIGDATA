from pyspark import SparkContext, SparkConf
"""
#------------1 way--------------------------------------------------------------------
conf = SparkConf.setAppName("youtube_demo").setMaster("local")
sc = SparkContext(conf=conf)
# print(sc.getConf().getAll())
sc.getConf().getAll()

#------------2nd way------------------------------------------------------

sc = SparkContext()
print(sc.getConf().getAll())
"""

# sc = SparkContext()
# # sc.setLogLevel("WARN")
# names=sc.parallelize(['a','b','c',4,5,6])
# print(names.collect())

# from pyspark.sql import SparkSession
# spark=SparkSession.builder.appName('Basics').getOrCreate()
# import pyspark
# from pyspark.sql.types import StructField,StringType,IntegerType,StructType
# data_schema=[StructField('age',IntegerType(),True),
#              StructField('firstname',StringType(),True),
#             StructField('lastname',StringType(),True),
#             StructField('gender',StringType(),True),
#             StructField('number',IntegerType(),True)]
# final_struct=StructType(fields=data_schema)
# df=spark.read.json('D:\software_installation\Pycharm_files\sample4.json',schema=final_struct)
# df.show()

from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('Basics').getOrCreate()
# df=spark.read.json('D:\software_installation\Pycharm_files\sample4.json').cache()
df = spark.read.option("multiline","true").json('D:\software_installation\Pycharm_files\sample4.json')
df.head(3)[0]
# df.printSchema()
# df.columns