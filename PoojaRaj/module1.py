from pyspark import SparkContext
from sys import stdin
# common lines
if __name__=="__main__":

    sc=SparkContext("local[*]","WordCount")
    # sc.setLogLevel("ERROR")
    input= sc.textFile("D:/Poojasparkcode/wordcount.txt")
    words=input.flatMap(lambda x:x.split(" "))
    word_Counts=words.map(lambda x:(x,1))
    final_count=word_Counts.reduceByKey(lambda x,y:x+y)
    result=final_count.collect()
    for a in result:
        print(a)
else:
    print("Invoked indirectly")