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

new_pair_rdd = original_logs_rdd.map(lambda x: (x.split(":")[0], 1))
new_pair_rdd.mapValues(lambda x: x)

# mapValues and flatMapValues work on pair Rdd. It will work on value and key will remain as it is.
print("mapvalues Data")
for x in new_pair_rdd.collect():
    print(x)

print("flatmapvalues Data")
new_pair_rdd.flatMapValues(lambda x: x)
for x in new_pair_rdd.collect():
    print(x)

# flatMap work similar to explode in DF .
print("flatmap Data")
new_pair_flatmap = original_logs_rdd.flatMap(lambda x: x.split(":"))
for x in new_pair_flatmap.collect():
    print(x)

resultant_rdd = new_pair_rdd.reduceByKey(lambda x, y: x + y)
for x in resultant_rdd.collect():
    print(x)

# In below case flatmap will behave different way if lambda function is given as tuple. o/p will list,1.
new_pair_flatmap = original_logs_rdd.flatMap(lambda x: (x.split(":"),1))
for x in new_pair_flatmap.collect():
    print(x)