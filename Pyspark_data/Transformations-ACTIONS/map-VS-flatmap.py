#map behaves as extend . flatmap as append in python
import gettext

from pyspark import SparkContext
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName('map').getOrCreate()
sc=spark.sparkContext
in_rdd=sc.textFile("D:\Movie/abc.txt")
"""
print(in_rdd.collect())

rdd_map=in_rdd.map(lambda x:x.split(","))
print(rdd_map.collect())

rdd_map=in_rdd.flatMap(lambda x:x.split(","))
print(rdd_map.collect())
"""


data = [('James','Smith','M',3000),
  ('Anna','Rose','F',4100),
  ('Robert','Williams','M',6200),
]
columns = ["firstname","lastname","gender","salary"]
spark=SparkSession.builder.appName("employee").getOrCreate()
df10 = spark.createDataFrame(data=data, schema = columns)
rdd2=df10.rdd.map(lambda x:(x[0]+','+x[1],x[2]))
df2=rdd2.toDF(["fullname",'gender'])
# df2.show()
def func1(x):
    firstName=x.firstname
    lastName=x.lastname
    name=firstName+","+lastName
    gender=x.gender.lower()
    salary=x.salary*2
    return (name,gender,salary)

rdd3=df10.rdd.map(lambda x: func1(x))
rdd3.toDF(['name','gender','salary']).show()

# mapPartitions iterate through each partition

data = [('James','Smith','M',3000),
  ('Anna','Rose','F',4100),
  ('Robert','Williams','M',6200),
]

columns = ["firstname","lastname","gender","salary"]
df = spark.createDataFrame(data=data, schema = columns)
df.show()
def reformat(partitionData):
    for row in partitionData:
        yield [row.firstname+","+row.lastname,row.salary*10/100]

df2=df.rdd.mapPartitions(reformat).toDF(["name","bonus"])
df2.show()


#it will show partitionindex and length of element it contains
# df2=df.repartition(4).rdd.mapPartitionsWithIndex(lambda (index,x):(index,x.length)).toDF("index","psize").show()







