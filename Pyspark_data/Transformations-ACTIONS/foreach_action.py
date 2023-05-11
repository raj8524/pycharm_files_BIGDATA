from pyspark.sql import SparkSession
from pyspark.sql.functions import lit,col,when,explode,flatten,map_keys,map_values,create_map,expr,regexp_replace,array_contains,udf
from pyspark.sql.types import StructType,StructField,StringType,MapType,IntegerType,ArrayType,BooleanType
data = [('James','Smith','M',3000),
  ('Anna','Rose','F',4100),
  ('Robert','Williams','M',6200),
]

#foreach is row based action where as map is transformation.

columns = ["firstname","lastname","gender","salary"]
spark=SparkSession.builder.appName("employee").getOrCreate()
df10 = spark.createDataFrame(data=data, schema = columns)
def f(x):
    print(x)
df10.foreach(f)

