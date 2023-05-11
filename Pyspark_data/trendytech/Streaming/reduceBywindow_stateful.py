from pyspark import *
from pyspark.streaming import *
# nc -lk 8888
sc = SparkContext("local[2]", "APP")
sc.setLogLevel("ERROR")
# creating spark streaming context
ssc = StreamingContext(sc, 2)
# ssc.checkpoint("D://software_installation//Pycharm_files//Pyspark_data//trendytech//checkpoint")
ssc.checkpoint("checkpoint")
# lines is a dstream
# lines = ssc.socketTextStream("localhost", 8888)
lines = ssc.socketTextStream("51.161.115.223", 8896)

def summaryfuct(x, y):
    return str((int(x) + int(y)))

def inversefuct(x, y):
    return str((int(x) - int(y)))

wordCounts = lines.reduceByWindow(summaryfuct, inversefuct, 10, 2)
wordCounts.pprint()
ssc.start()
ssc.awaitTermination()
