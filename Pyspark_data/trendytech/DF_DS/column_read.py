
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
data = [("James","Smith","USA","CA"),
    ("Michael","Rose","USA","NY"),
    ("Robert","Williams","USA","CA"),
    ("Maria","Jones","USA","FL")
  ]
columns = ["firstname","lastname","country","state"]
df = spark.createDataFrame(data = data, schema = columns)
# df.show(truncate=False)
# df.select(df.colRegex("`^.*name*`")).show()


# Select All columns from List
# df.select(*columns).show()

# Select All columns
# df.select([col for col in df.columns]).show()
# df.select("*").show()
#
# df.select(df.columns[2:4]).show()
#
# df.select("name.*").show(truncate=False)
