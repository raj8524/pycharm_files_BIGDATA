from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
spark=SparkSession.builder.appName("records").getOrCreate()
df_emp=spark.read.format("csv").option("header","True").option("inferSchema","True").option("path","D:/employee.csv").load()
df_dept=spark.read.format("csv").option("header","True").option("inferSchema","True").option("path","D:/dept.csv").load()
# df_emp.show()
# df_dept.show()
df_emp.createOrReplaceTempView("emp")
df_dept.createOrReplaceTempView("dept")
# spark.sql("""select e.emp_id,e.dept_id,e.salary,d.dept_name,dense_rank() over(partition by e.dept_id order by e.salary desc) A from emp e join dept d
# on e.dept_id=d.dept_id""").filter(col("A")==1).show()
windowspec=Window.partitionBy("dept_id1").orderBy("salary")
df_select=df_emp.withColumn("rank1",dense_rank().over(windowspec))
df_select.show()
df_join=df_select.join(df_dept,df_select.dept_id1 == df_dept.dept_id,"inner").select("emp_id","dept_id1","salary","dept_name","rank1")
df_join.show()
