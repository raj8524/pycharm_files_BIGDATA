import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number,rank,dense_rank,ntile,cume_dist,percent_rank,col, avg, sum, min, max, row_number,to_date,lag,year,expr,when
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

"""
simpleData = (("James", "Sales", 3000,4000), ("Michael", "Sales", 4600,4600), ("Robert", "Sales", 4100,4100), ("Maria", "Finance", 3300,3300),
              ("James", "Sales", 3000,5000), ("Scott", "Finance", 3300,3300), ("Jen", "Finance", 3900,3900), ("Jeff", "Marketing", 3000,3000),
              ("Kumar", "Marketing", 2000,2000),("Saif", "Sales", 4100,4100),("Rani", "IT", 4100,7000),("karan", "IT", 4100,4100),
              ("Raj", "IT", 4100,7000),("Golu", "Sales", 3000,5000))

columns = ["employee_name", "department", "salary","bonus"]

df = spark.createDataFrame(data=simpleData, schema=columns)
# df.show(truncate=False)
windowSpec = Window.orderBy("salary","bonus") 
df.withColumn("dense_rank", dense_rank().over(windowSpec)).show(truncate=False)

windowSpec = Window.partitionBy("department").orderBy("salary","bonus")
df.withColumn("dense_rank", dense_rank().over(windowSpec))\
    .select("employee_name", "department", "salary","bonus","dense_rank")\
    .where(col("dense_rank")>3).show(truncate=False)

df.createOrReplaceTempView("employee")
spark.sql("select employee_name,department,dense_rank() over(partition by department order by salary,bonus) "
          "as dense,salary,bonus from employee").show()

spark.sql("select employee_name,department,rank() over(partition by department order by salary,bonus) "
          "as rank,salary,bonus from employee").show()
spark.sql("select employee_name,department,row_number() over(partition by department order by salary,bonus) "
          "as row_number,salary,bonus from employee").show()

spark.sql("select employee_name,department,salary,dense_rank from(select employee_name,department,dense_rank() over(partition by department order by salary,bonus)"
          "as dense_rank,salary,bonus from employee) as emp1 where dense_rank>3")


df.withColumn("rank", rank().over(windowSpec)) \
    .show()

df.withColumn("dense_rank", dense_rank().over(windowSpec)) \
    .show()

df.withColumn("percent_rank", percent_rank().over(windowSpec)) \
    .show()

df.withColumn("ntile", ntile(2).over(windowSpec)) \
    .show()

df.withColumn("cume_dist", cume_dist().over(windowSpec)) \
    .show()

from pyspark.sql.functions import lag

df.withColumn("lag", lag("salary", 2).over(windowSpec)) \
    .show()

from pyspark.sql.functions import lead

df.withColumn("lead", lead("salary", 2).over(windowSpec)) \
    .show()

windowSpecAgg = Window.partitionBy("department")

df.withColumn("row", row_number().over(windowSpec)) \
    .withColumn("avg", avg(col("salary")).over(windowSpecAgg)) \
    .withColumn("sum", sum(col("salary")).over(windowSpecAgg)) \
    .withColumn("min", min(col("salary")).over(windowSpecAgg)) \
    .withColumn("max", max(col("salary")).over(windowSpecAgg)) \
    .where(col("row") == 1).select("department", "avg", "sum", "min", "max").show()
"""
departments=[(10,"IT",100),(10,"IT",100),(10,"IT",100),(20,"HR",201),(20,"HR",202),(20,"HR",203),(30,"Tester",301),(30,"Tester",302),
             (40,"finance",401),(40,"finance",402),(50,"software",501),(50,"software",502),(50,"software",503),(60,"server",601),(60,"server",602),(60,"server",604)]
dep_schema=("dep_id","dep_name","dep_desc")
dep_DF=spark.createDataFrame(data=departments,schema=dep_schema)
dep_DF.createOrReplaceTempView("departments")
# dep_DF.show()
employees=[(1001,"raj","kumar","abc@gmail.com",9876,3000,10),(1002,"raja","kernal","ab@gmail.com",98765,3020,10),(1003,"rani","kumari","db@gmail.com",98761,3220,10),
           (2001,"raju","mare","acc@ibm.com",9877,2000,20),(2002,"ranu","mandal","cb@ibm.com",98755,2020,20),(2003,"ravi","kalu","eb@ibm.com",98762,2220,20),
           (4001,"jaya","kumari","abd@hp.com",8876,4000,40),(4002,"janvi","khemu","eab@hp.com",88765,4020,40),(4003,"rajvir","kumar","fb@hp.com",68761,4220,40),
           (6001,"rajan","kumar","gbc@acc.in",9875,6000,60),(6002,"rajni","aarya","hb@acc.in",98325,6020,60),(6003,"rohit","gupta","kb@acc.in",98741,6220,60)]
emp_shcema=("emp_id","first_name","last_name","email","phone","salary","dep_id")
emp_DF=spark.createDataFrame(data=employees,schema=emp_shcema)
emp_DF.createOrReplaceTempView("employees")
# emp_DF.show()
evaluations=[(9551,1001,"10/03/2021","Science1","Sience",305),(9553,1002,"11/03/2021","Science3","Sience",315),(9555,1003,"13/03/2021","Science2","Sience",325),
             (9661,2001,"10/04/2021","bio1","bio",305),(9663,2002,"11/04/2021","bio2","bio",315),(9665,2003,"13/04/2021","bio3","bio",325),
             (9771,4001, "10/05/2021", "maths1", "Maths", 405),(9773,4002, "11/05/2021", "maths2", "Maths", 415),(9555,4003, "13/03/2021", "maths3", "Maths", 425),
             (9881,6001, "10/06/2022", "Hindi1", "Hindi", 375), (9663,6002, "11/06/2021", "Hindi2", "Hindi", 385),(9665,6003, "13/06/2021","Hindi3", "Hindi", 395)]
eval_schema=("evail_id","emp_id","eval_date","eval_name","notes","marks")
eval_DF=spark.createDataFrame(data=evaluations,schema=eval_schema)
eval_DF.createOrReplaceTempView("evaluations")
# eval_DF.show()
overtime=[(801,1001,"10-03-2021",4),(801,1002,"10-03-2021",4),(801,1003,"11-03-2021",4),(801,4001,"11-03-2021",4),(801,4003,"13-03-2021",4),
          (801,6001,"11-05-2021",5),(801,6003,"11-05-2021",6),(801,6005,"11-06-2021",7),(801,2001,"13-06-2021",8),(801,2003,"13-04-2021",4)]
ot_schema=("otime_id","emp_id","otime_date","no_of_hours")
ot_DF=spark.createDataFrame(data=overtime,schema=ot_schema)
ot_DF.createOrReplaceTempView("overtime")
# ot_DF.show()

# spark.sql("SELECT emp_id,to_date(eval_date,'MM/dd/yyyy') date1,eval_name,"
# 	"marks,LAG(marks) OVER (PARTITION BY notes ORDER BY eval_date) AS previous FROM evaluations").show()

# spark.sql("SELECT emp_id,year,eval_name,marks,previous,
# 	"IF (previous = 0, '0%',CONCAT(ROUND((marks - previous)*100/previous, 2), '%')) AS difference FROM(SELECT emp_id,YEAR(to_date(eval_date,'MM/dd/yyyy')) year,eval_name,marks,"
#           "LAG(marks,1,0) OVER (PARTITION BY emp_id ORDER BY eval_date) AS previous FROM evaluations)").show()
#
# spark.sql("SELECT emp_id,year,eval_name,marks,previous,"
# 	"IF (previous = 0) THEN 0% ELSE CONCAT(ROUND((marks - previous)*100/previous, 2), '%') AS difference FROM(SELECT emp_id,YEAR(to_date(eval_date,'MM/dd/yyyy')) year,eval_name,marks,"
#           "LAG(marks,1,0) OVER (PARTITION BY emp_id ORDER BY eval_date) AS previous FROM evaluations)").show()

#==============================================================LAG MARKS PERCENTAGE
eval_window=Window.partitionBy("notes").orderBy("eval_date")
eval_lag1=eval_DF.withColumn("previous",lag("marks",1).over(eval_window)).show()

eval_lag=eval_DF.withColumn("previous",lag("marks",1,0).over(eval_window))
eval_lag.selectExpr("year(to_date(eval_date,'MM/dd/yyyy')) YEAR","emp_id","notes","marks","previous")\
    .withColumn('difference',when(col("previous") == 0,'0%')
                             .otherwise(expr("CONCAT(ROUND((marks - previous)*100/previous, 2), '%')"))).show()
