from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('Basics').getOrCreate()
df=spark.read.csv()
# df=spark.read.json('D:\software_installation\Pycharm_files\sample4.json')
df = spark.read.option("multiline","true").json('D:\software_installation\Pycharm_files\sample4.json')
df.show()
df.printSchema()
df.columns
df.describe().show()

from pyspark.sql.types import StructField,StringType,IntegerType,StructType
data_schema=[StructField('age',IntegerType(),True),
             StructField('name',StringType(),True)]
final_struct=StructType(fields=data_schema)
df=spark.read.json('D:\software_installation\Pycharm_files\sample4.json',schema=final_struct)
df.printSchema()
type(df['age'])
df.select('age').show() # will show age values
df.head(2)[0] #will show top 2 values of which 0th row value will show
type(df.select('age'))
type(df.head(2)[0])  # will show type of like row
df.select(['age','name']).show() # will show age ,name value as list of column is passed
df.withColumn('newage',df('age')).show() #will form newage column similar to age and its values
df.withColumn('newage',df['age']*2).show() # new columan will be double value than old as *2
df.withColumnRenamed('age','newage_data').show() #age column is renamed to newage_data
df.createOrReplaceTempView('people')#temporary temp view is formed
results=spark.sql('select * from people')
results.show()
new_results=spark.sql('select * from people where age=30')
new_results.show()


#csv files----------
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('Basics').getOrCreate()
df=spark.read.csv('D:\software_installation\Pycharm_files\Sample1.csv',inferSchema=True,header=True)
df.na.drop(how='any').show()

