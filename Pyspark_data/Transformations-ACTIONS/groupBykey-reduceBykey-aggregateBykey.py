from pyspark.sql import SparkSession
spark=SparkSession.builder.master("local").appName("keys").getOrCreate()
sc=spark.sparkContext
in_text_rdd=sc.textFile("D:\Study\pyspark-docs/abc.txt")
in_text_rdd.collect()

text_count=in_text_rdd.flatMap(lambda x:x.split(","))\
                      .map(lambda x:(x,1))\
                      .reduceByKey(lambda x,y:x+y)
# print(text_count.collect())

text_count1=in_text_rdd.flatMap(lambda x:x.split(","))\
                      .map(lambda x:(x,1))
# print(text_count1.collect())

text_count2=in_text_rdd.flatMap(lambda x:x.split(","))\
                      .map(lambda x:(x,1))\
                      .groupByKey()\
                      .mapValues(lambda x:list(x))
for key,val in text_count2.collect():
    print(key,sum(val))


its gud to study any college but streams r important. important streams r IT,CSE.