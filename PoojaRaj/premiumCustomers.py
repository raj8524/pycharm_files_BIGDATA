from pyspark.storagelevel import StorageLevel

from pyspark import SparkContext
sc=SparkContext("local[*]","PremiumCustomers")
base_rdd=sc.textFile("D:/Poojasparkcode/customerorders.csv")
mapped_rdd=base_rdd.map(lambda x:(x.split(",")[0],float(x.split(",")[2])))
total_by_customer=mapped_rdd.reduceByKey(lambda x,y: x+y)
premium_customers=total_by_customer.filter(lambda x:x[1] > 5000)
doubled_amount=premium_customers.map(lambda x:(x[0],x[1]*2)).persist(StorageLevel.MEMORY_ONLY)
result=doubled_amount.collect()
for x in result:
    print(x)
print(doubled_amount.count())