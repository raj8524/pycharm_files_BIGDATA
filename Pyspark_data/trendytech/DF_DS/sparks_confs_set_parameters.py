from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
def config_values():
    my_conf = SparkConf()
    my_conf.set("spark.app.name", "my first application")
    my_conf.set("spark.master","local[*]")
    my_conf.set("spark.sql.adaptive.enabled","True")
    my_conf.set("spark.sql.adaptive.skewJoin.enabled","True")
    my_conf.set("spark.sql.adaptive.coalescePartitions.enabled","True")
    my_conf.set("spark.sql.shuffle.partitions","300")
    my_conf.set("spark.scheduler.mode","FAIR")
    my_conf.set("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored","true")
    my_conf.set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    my_conf.set("spark.driver.memory","195g")
    my_conf.set("spark.sql.broadcastTimeout","36000")
    my_conf.set("spark.sql.codegen.wholeStage","false")
    my_conf.set("spark.dynamicAllocation.enabled","true")
    my_conf.set("spark.shuffle.service.enabled","true")
    my_conf.set("spark.sql.shuffle.partitions","1800")
    my_conf.set("spark.dynamicAllocation.minExecutors","1")
    my_conf.set("spark.dynamicAllocation.maxExecutors","1800")
    my_conf.set("spark.stage.maxConsecutiveAttempts","10"),
    my_conf.set("spark.rpc.io.serverTreads","64")
    my_conf.set("spark.shuffle.service.index.cache.size","2048")
    my_conf.set("spark.shuffle.file.buffer","1M")
    my_conf.set("spark.unsafe.sorter.spill.reader.buffer.size","1M")
    my_conf.set("spark.memory.offHeap.enable","true")
    my_conf.set("spark.memory.ofHeap.size","20g")
    my_conf.set("spark.speculation","false")
    my_conf.set("maximizeResourceAllocation","true")
    my_conf.set("spark.rdd.compress","true")
    my_conf.set("spark.shuffle.compress","true")
    my_conf.set("spark.shuffle.spill.compress","true")
    my_conf.set("spark.serializer:","org.apache.spark.serializer.KryoSerializer")
    my_conf.set("spark.kryoserializer.buffer.max","256M")
    my_conf.set("spark.kryoserializer.buffer","128M")
    my_conf.set("spark.sql.join.preferSortMergeJoin","true")
    my_conf.set("spark.driver.maxResultSize","5g")
    my_conf.set("spark.sql.parquet.filterPushdown","true")
    my_conf.set("spark.hadoop.parquet.filter.stats.enabled","true")
    my_conf.set("spark.sql.optimizer.nestedSchemaPruning.enabled","true")
    my_conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled","true")
    return my_conf

input_data = [(1, "Shivansh", "Male", (70, 66, 78, 70, 71, 50), 80,"Good Performance"),
(2, "Arpita", "Female", (20, 16, 8, 40, 11, 20), 18,"Have to work hard otherwise result will not improve"),
(3, "Raj", "Male", (10, 26, 28, 10, 31, 20),21, "Work hard can do better"),
(4, "Swati", "Female", (70, 66, 78, 70, 71, 50),69, "Good performance can do more better"),
(5, "Arpit", "Male", (20, 46, 18, 20, 31, 10),20, "Focus on some subject to improve"),
(6, "Swaroop", "Male", (70, 66, 48, 30, 61, 50),65, "Good performance"),
(7, "Reshabh", "Male", (70, 66, 78, 70, 71, 50),70, "Good performance"),
(8, "Dinesh", "Male", (40, 66, 68, 70, 71, 50),65, "Can do better"),
(9, "Rohit", "Male", (50, 66, 58, 50, 51, 50),55, "Can do better"),
(10, "Sanjana", "Female", (60, 66, 68, 60, 61, 50),67, "Have to work hard")]

schema = ["ID", "Name", "Gender","Sessionals Marks", "Percentage", "Remark"]

def create_session():
    spk = SparkSession.builder.config(conf=config_values()).getOrCreate()
    return spk

def create_df(spark, data, schema):
    df1 = spark.createDataFrame(data, schema)
    return df1


if __name__ == "__main__":
    # calling function to create SparkSession
    spark = create_session()
    invoiceDF = create_df(spark, input_data, schema)
    invoiceDF.show()

