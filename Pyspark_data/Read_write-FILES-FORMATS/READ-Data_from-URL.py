import pyspark
from pyspark import SparkContext
from pyspark import SparkFiles
from pyspark.sql import SparkSession
spark=SparkSession.builder.master("local").appName("keys").getOrCreate()
url="https://raw.githubusercontent.com/azar-s91/dataset/master/BankChurners.csv"
spark.sparkContext.addFile(url)
# SparkFiles.get("BankChurners.csv")   gives local system location where data from url is stored
df_url=spark.read.option("header","true").option("inferSchema","True").csv(SparkFiles.get("BankChurners.csv"))
df_url.show()



#m2--------------
from urllib.request import urlopen

http=urlopen('https://raw.githubusercontent.com/azar-s91/dataset/master/BankChurners.csv').read.decode('utf-8')
sc=spark.sparkContext
rdd_str=sc.parallelize([http])
dfread=spark.read.json(rdd_str)
dfread.show()