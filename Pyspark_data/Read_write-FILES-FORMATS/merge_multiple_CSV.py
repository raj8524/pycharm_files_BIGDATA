#reading it from D folder and files start with sample
import pyspark
from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
conf=pyspark.SparkConf().setAppName("samples").setMaster("local")
sc=pyspark.SparkContext(conf=conf)
spark=SparkSession(sc)
sql_c=SQLContext(sc)
df=sql_c.read.csv('D:\sample*')
df.show()