from pyspark import SparkContext
from sys import stdin
def parse_line(line):
    fields=line.split(",")
    content=fields[0].lower()
    pricepercontent=float(fields[10])
    return (pricepercontent,content)

sc = SparkContext("local[*]", "bigdatacampaigndata")
sc.setLogLevel("ERROR")
input = sc.textFile("D:/Poojasparkcode/bigdatacampaigndata.csv")
mapped_Input=input.map(parse_line)
words=mapped_Input.flatMapValues(lambda x:(x.split(" ")))
final_Mapped=words.map(lambda x:(x[1],x[0]))
total=final_Mapped.reduceByKey(lambda x,y:(x+y)).sortBy(lambda x:x[1],False)
result=total.take(20)
for a in result:
    print(a)