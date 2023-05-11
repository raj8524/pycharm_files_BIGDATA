# from pyspark import SparkConf
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import *
# my_conf=SparkConf()
# my_conf.set("spark.app.name","first application")
# my_conf.set("spark.master","local[*]")
# spark=SparkSession.builder.config(conf=my_conf).getOrCreate()
# accu=spark.sparkContext.accumulator(0)
# myList =[(1,"2013-07-25",11599,"CLOSED"),
#     (2,"2014-07-25",256,"PENDING_PAYMENT"),
#     (3,"2013-07-25",11599,"COMPLETE"),
#     (4,"2019-07-25",8827,"CLOSED")]
# df=spark.createDataFrame(myList).toDF("orderid","orderdate","customerid","status")
#
# df.show()
from test import xyz
y={
    "name":"raj",
    "age":30,
    "address":{"city":"ssm"}
}
z=(4,4,5)
a=10
xyz(a,*z,**y)


