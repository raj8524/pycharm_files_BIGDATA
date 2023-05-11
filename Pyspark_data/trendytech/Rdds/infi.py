from pyspark.sql import SparkSession
from pyspark.sql.functions import lit,col,when,explode,flatten,map_keys,map_values,create_map,expr,regexp_replace,array_contains

from pyspark.sql.types import StructType,StructField,StringType,MapType,IntegerType,ArrayType
emp_data1=[(25,"pooja","sanger",{"hair":"red","eye":"black"},"BI z999",7001,10000,"Bihar",101,["java","c++","py"]),
     (25,"ankita","raj",{"hair":"brown","eye":"brown"},"HP 140AC",8001,3000,"HP",201,[]),
     (25,"ravi","gulati",{"hair":"white","eye":"black"},"AP bc91",15001,7000,"AP",301,["java","c++","py"])]

emp_data2=[(25,"pooja","sanger",{"hair":"red","eye":"black"},"BI z999",7001,10000,"Bihar",101,["java","c++","py"]),
     (25,"ankita","raj",{"hair":"brown","eye":"brown"},"HP 140AC",8001,3000,"HP",201,[]),
     (25,"ravi","gulati",{"hair":"white","eye":"black"},"AP bc91",15001,7000,"AP",301,["java","c++","py"])]

schema=StructType([StructField("emp_id",IntegerType(),True),StructField("fname",StringType(),True),StructField("lname",StringType(),True),
                 StructField("color",MapType(StringType(),StringType()),True),StructField("address",StringType(),True),
                 StructField("salary",IntegerType(),True),StructField("bonus",IntegerType(),True),
                 StructField("state",StringType(),True),StructField("dept_id",IntegerType(),True),
                      StructField("Lang",ArrayType(StringType()),True)])
spark=SparkSession.builder.appName("employee").getOrCreate()
emp_df1=spark.createDataFrame(data=emp_data1,schema=schema)
emp_df1.createOrReplaceTempView("emp1")
emp_df2=spark.createDataFrame(data=emp_data2,schema=schema)
emp_df2.createOrReplaceTempView("emp2")
spark.sql("""select emp1.emp_id from emp1 full outer join emp2 using(emp_id)""").show()



leftsemi join
+------+--------+---------------+-----------+-----------+------+------+
|emp_id|name    |superior_emp_id|year_joined|emp_dept_id|gender|salary|
+------+--------+---------------+-----------+-----------+------+------+
|1     |Smith   |-1             |2018       |10         |M     |3000  |
|2     |Rose    |1              |2010       |20         |M     |4000  |
|3     |Williams|1              |2010       |10         |M     |1000  |
|4     |Jones   |2              |2005       |10         |F     |2000  |
|5     |Brown   |2              |2010       |40         |      |-1    |
+------+--------+---------------+-----------+-----------+------+------+
