from pyspark import *
from pyspark .streaming import *
sc = SparkContext("local[2]","APP")
sc.setLogLevel("ERROR")
#creating spark streaming context
ssc = StreamingContext(sc, 2)
ssc.checkpoint("D://software_installation//Pycharm_files//Pyspark_data//trendytech//checkpoint")
#lines is a dstream
# lines = ssc.socketTextStream("localhost", 9998)
lines = ssc.socketTextStream("51.161.115.223", 8889)
#words is a transformed dstream
wordCounts = lines.flatMap(lambda x: x.split()) \
    .map(lambda x: (x, 1)) \
    .reduceByKeyAndWindow(lambda x, y: int(x) + int(y), lambda x, y: int(x) -int(y), 10, 2)
wordCounts.pprint()
ssc.start()
ssc.awaitTermination()
