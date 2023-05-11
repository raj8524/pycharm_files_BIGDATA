from pyspark import SparkContext
sc = SparkContext("local[*]", "logLevelCount")
sc.setLogLevel("ERROR")
if __name__ == "__main__":
    my_list = ["WARN: Tuesday 4 September 0405",
    "ERROR: Tuesday 4 September 0408",
    "info: Tuesday 4 September 0408",
    "ERROR: Tuesday 4 September 0408",
    "warn: Tuesday 4 September 0408",
    "warn: Tuesday 4 September 0408"]
    original_logs_rdd = sc.parallelize(my_list)
else:
    original_logs_rdd = sc.textFile("D://Study//TrendyTechInsight//Sparkweek10//bigLog.txt")
    print("inside the else part")
    
new_pair_rdd = original_logs_rdd.map(lambda x:(x.split(":")[0],1))
resultant_rdd = new_pair_rdd.reduceByKey(lambda x,y: x+y)
for x in resultant_rdd.collect():
    print(x)

