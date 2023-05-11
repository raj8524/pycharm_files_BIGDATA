from pyspark import SparkContext
sc = SparkContext("local[*]", "logLevelCount")
sc.setLogLevel("ERROR")
base_rdd = sc.textFile("D://Study//TrendyTechInsight//Sparkweek10//bigdatacampaigndata.csv")
mapped_rdd = base_rdd.map(lambda x: (x.split(":")[0], x.split(":")[1]))
grouped_rdd = mapped_rdd.groupByKey()
final_rdd = grouped_rdd.map(lambda x: (x[0], len(x[1])))
result = final_rdd.collect()
for x in result:
    print(x)