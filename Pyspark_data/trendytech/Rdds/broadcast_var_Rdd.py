from pyspark import SparkContext
sc=SparkContext("local[*]","broadcast")
def removeBoringWords():
    nameset=set(line.strip() for line in open("D:\\Study\\TrendyTech Insight\\Spark week 10\\boringwords.txt"))
    return nameset

broadcasted=sc.broadcast(removeBoringWords())
readFile=sc.textFile("D:\\Study\\TrendyTech Insight\\Spark week 10\\bigdatacampaigndata.csv")
selected_cols=readFile.map(lambda y :(float(y.split(',')[10]),y.split(',')[0]))
splitString=selected_cols.flatMapValues(lambda z:(z.split(" ")))
changeindex=splitString.map(lambda x:(x[1],x[0]))
filteredWords=changeindex.filter(lambda x:x[0] not in broadcasted.value)
reduceSum=filteredWords.reduceByKey(lambda y,z:y+z)
for i in reduceSum.sortBy(lambda x:x[1],False).take(30):
    print(i)