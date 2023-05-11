from pyspark import SparkContext
sc = SparkContext("local[*]","KeywordAmount")
sc.setLogLevel("ERROR")
count=0
def loadBoringWords():
    boring_words = set(line.strip() for line in
    open("D://Study//TrendyTechInsight//Sparkweek10//boringwords.txt"))
    # print(len(boring_words))
    return boring_words

name_set = sc.broadcast(loadBoringWords())
initial_rdd = sc.textFile("D://Study//TrendyTechInsight//Sparkweek10//bigdatacampaigndata.csv")
mapped_input = initial_rdd.map(lambda x: (float(x.split(",")[10]),x.split(",")[0]))
# rdd=mapped_input.collect()
words1=mapped_input.flatMap(lambda x:(x[1].split(" "),x[0]))
for x in words1.collect():
    print(x)
words = mapped_input.flatMapValues(lambda x: x.split(" "))
rdd=words.collect()
for x in rdd:
    print(x)
final_mapped = words.map(lambda x: (x[1].lower(),x[0]))
filtered_rdd = final_mapped.filter(lambda x: x[0] not in name_set.value)
total = filtered_rdd.reduceByKey(lambda x,y: x+y)
sorted = total.sortBy(lambda x: x[1],False)
result = sorted.take(20)
# for x in result:
#     print(x)