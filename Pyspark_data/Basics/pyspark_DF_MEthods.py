import pyspark,pandas
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

#select,distinct are transformation which result in new df where as filter,collect,show r actions.

# -------------------------------------                   rdd to DATAFRAME  -----------------------------------
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("kaju").getOrCreate()
data=[("kaju",2),("jalebi",3)]
rdd2=spark.sparkContext.parallelize(data)
df=rdd2.toDF()
df.show()

#-----------------------------------------------------------------------------convert pyspark data frame to pandas -----------------------
"""

from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("kaju").getOrCreate()
data=[("kaju",2),("jalebi",3)]
df3=spark.createDataFrame(data)
df3.show()
dfpanda=df3.toPandas()
print(dfpanda)


data = [("James","","Smith","36636","M",60000),
        ("Michael","Rose","","40288","M",70000),
        ("Robert","","Williams","42114","",400000),
        ("Maria","Anne","Jones","39192","F",500000),
        ("Jen","Mary","Brown","","F",0)]
columns = ["first_name","middle_name","last_name","dob","gender","salary"]
pysparkDF = spark.createDataFrame(data = data, schema = columns)
pysparkDF.printSchema()
pysparkDF.show(truncate=False)
pandasDF = pysparkDF.toPandas()
print(pandasDF)


#show func in data frame    truncate=False to show all data
def show(self, n=20, truncate=True, vertical=False):
    pass



# structtype,structfield,struct string
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.master("local[1]") \
    .appName('SparkByExamples.com') \
    .getOrCreate()

data = [("James", "", "Smith", "36636", "M", 3000),
        ("Michael", "Rose", "", "40288", "M", 4000),
        ("Robert", "", "Williams", "42114", "M", 4000),
        ("Maria", "Anne", "Jones", "39192", "F", 4000),
        ("Jen", "Mary", "Brown", "", "F", -1)
        ]

schema = StructType([ \
    StructField("firstname", StringType(), True), \
    StructField("middlename", StringType(), True), \
    StructField("lastname", StringType(), True), \
    StructField("id", StringType(), True), \
    StructField("gender", StringType(), True), \
    StructField("salary", IntegerType(), True) \
    ])

df = spark.createDataFrame(data=data, schema=schema)
df.printSchema()
df.show(truncate=False)
"""

#---------------------------------------------------------------------------- ROW CLASS -------------------------------------
from pyspark.sql import Row
row=Row("James",40)
print(row[0] +","+str(row[1]))

row=Row(name="Alice", age=11)
print(row.name)

from pyspark.sql import SparkSession, Row

row = Row("James", 40)
print(row[0] + "," + str(row[1]))
row2 = Row(name="Alice", age=11)
print(row2.name)

Person = Row("name", "age")
p1 = Person("James", 40)
p2 = Person("Alice", 35)
print(p1.name + "," + p2.name)

# PySpark Example
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

data = [Row(name="James,,Smith", lang=["Java", "Scala", "C++"], state="CA"),
        Row(name="Michael,Rose,", lang=["Spark", "Java", "C++"], state="NJ"),
        Row(name="Robert,,Williams", lang=["CSharp", "VB"], state="NV")]

# RDD Example 1
rdd = spark.sparkContext.parallelize(data)
collData = rdd.collect()
print(collData)
for row in collData:
    print(row.name + "," + str(row.lang))

# RDD Example 2
Person = Row("name", "lang", "state")
data = [Person("James,,Smith", ["Java", "Scala", "C++"], "CA"),
        Person("Michael,Rose,", ["Spark", "Java", "C++"], "NJ"),
        Person("Robert,,Williams", ["CSharp", "VB"], "NV")]
rdd = spark.sparkContext.parallelize(data)
collData = rdd.collect()
print(collData)
for person in collData:
    print(person.name + "," + str(person.lang))

# DataFrame Example 1
columns = ["name", "languagesAtSchool", "currentState"]
df = spark.createDataFrame(data)
df.printSchema()
df.show()

collData = df.collect()
print(collData)
for row in collData:
    print(row.name + "," + str(row.lang))

# DataFrame Example 2
columns = ["name", "languagesAtSchool", "currentState"]
df = spark.createDataFrame(data).toDF(*columns)
df.printSchema()

# Data frame columns

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
data = [("James","Smith","USA","CA"),
    ("Michael","Rose","USA","NY"),
    ("Robert","Williams","USA","CA"),
    ("Maria","Jones","USA","FL")
  ]
columns = ["firstname","lastname","country","state"]
df = spark.createDataFrame(data = data, schema = columns)
# df.select(df.firstname,df.state).show()
# df.select("firstname","state").show()
# xy=df.select(df["country"],df["lastname"]).collect()
# xy[1]
# df.select(*columns).show()
# z=df.collect()
# df.select("*").show()
# df.select([col for col in df.columns]).show()
# for i in z:
#     print(i)
# df.select(df.columns[:2]).show(2)
# df.select(df.columns[:]).show(2)
df.select("firstname").show(truncate=False)
dataCollect = df.select("dept_name").collect()
#select() is a transformation that returns a new DataFrame and holds the columns that are selected whereas collect() is
# an action that returns the entire data set in an Array to the driver.


# -------------------------------------------------------------Data frame with withcolumns,withColumnsRenamed, ------------------------------

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
data = [("James","Smith","USA","CA",101),
    ("Michael","Rose","USA","NY",201),
    ("Robert","Williams","USA","CA",101),
    ("Maria","Jones","USA","FL",301)
  ]
columns = ["firstname","lastname","country","state","code"]
df = spark.createDataFrame(data = data, schema = columns)
# df.withColumn("district",col("code").cast("integer")).show()
# df.withColumn("district",col("code")*10).show()
# df.withColumn("stopage",lit("thailand")).show()
df.withColumnRenamed("code","areacode").show()
df.drop("code").show()
# df.select("firstname").show(truncate=False)


from pyspark.sql.functions import *
df.select(col("name.firstname").alias("fname"), \
  col("name.middlename").alias("mname"), \
  col("name.lastname").alias("lname"), \
  col("dob"),col("gender"),col("salary")) \
  .printSchema()


# ------------------------------------------------------------------------------------------filter DF ------------------------------------------------------

df.filter(df.state == "OH") \
    .show(truncate=False)

df.filter(col("state") == "OH") \
    .show(truncate=False)

df.filter("gender  == 'M'") \
    .show(truncate=False)

df.filter((df.state == "OH") & (df.gender == "M")) \
    .show(truncate=False)

df.filter(array_contains(df.languages, "Java")) \
    .show(truncate=False)

df.filter(df.name.lastname == "Williams") \
    .show(truncate=False)
df.filter(df["state"]=="CA").show()
df.filter(~(df.state=="CA")).show()
df.filter(col("state")=="CA").show()
df.filter("state!='CA'").show()

li=["OH","CA","DE"]
df.filter(df.state.isin(li)).show()
df.filter(~df.state.isin(li)).show()
df.filter(df.state.isin(li)==False).show()
df.filter(df.state.startswith("N")).show()
df.filter(df.state.endswith("H")).show()
df.filter(df.state.contains("H")).show()
df.filter(df.name.like("%rose%")).show()

# ------------------------------------------------------------------------------------ Distinct, dropDuplicates()  -----------------------------------
#distinct,dropduplicates wont accpet dictionary columns
#Distinct
distinctDF = df.distinct()
print("Distinct count: "+str(distinctDF.count()))
distinctDF.show(truncate=False)

#Drop duplicates
df2 = df.dropDuplicates()
print("Distinct count: "+str(df2.count()))
df2.show(truncate=False)

#Drop duplicates on selected columns
dropDisDF = df.dropDuplicates(["department","salary"])
print("Distinct count of department salary : "+str(dropDisDF.count()))
dropDisDF.show(truncate=False)
}

#----------------------------------------------------------------order BY,sort by -------------------------------------------------
# sort,orderBy wont accept dictionay value type columns in bracket.
df.sort("department","state").show(truncate=False)
df.sort(col("department"),col("state")).show(truncate=False)

df.orderBy("department","state").show(truncate=False)
df.orderBy(col("department"),col("state")).show(truncate=False)


df.sort(df.department.asc(),df.state.desc()).show(truncate=False)
df.sort(col("department").asc(),col("state").desc()).show(truncate=False)
df.orderBy(col("department").asc(),col("state").desc()).show(truncate=False)

df.createOrReplaceTempView("EMP")
spark.sql("select employee_name,department,state,salary,age,bonus from EMP ORDER BY department asc").show(
    truncate=False)

# -------------------------------------------------------------------------Groupby ,where  -------------------------------------


simpleData = [("James", "Sales", "NY", 90000, 34, 10000),
("Michael", "Sales", "NY", 86000, 56, 20000),
("Robert", "Sales", "CA", 81000, 30, 23000),
("Maria", "Finance", "CA", 90000, 24, 23000),
("Raman", "Finance", "CA", 99000, 40, 24000),
("Scott", "Finance", "NY", 83000, 36, 19000),
("Jen", "Finance", "NY", 79000, 53, 15000),
("Jeff", "Marketing", "CA", 80000, 25, 18000),
("Kumar", "Marketing", "NY", 91000, 50, 21000)
]

schema = ["employee_name", "department", "state", "salary", "age", "bonus"]
df = spark.createDataFrame(data=simpleData, schema=schema)
df.printSchema()
df.show(truncate=False)

df.groupBy("department").sum("salary").show(truncate=False)

df.groupBy("department").count().show(truncate=False)

df.groupBy("department", "state") \
    .sum("salary", "bonus") \
    .show(truncate=False)

df.groupBy("department") \
    .agg(sum("salary").alias("sum_salary"), \
         avg("salary").alias("avg_salary"), \
         sum("bonus").alias("sum_bonus"), \
         max("bonus").alias("max_bonus") \
         ) \
    .show(truncate=False)

df.groupBy("department") \
    .agg(sum("salary").alias("sum_salary"), \
         avg("salary").alias("avg_salary"), \
         sum("bonus").alias("sum_bonus"), \
         max("bonus").alias("max_bonus")) \
    .where(col("sum_bonus") >= 50000) \
    .show(truncate=False)



# -------------------------------------------------------------------------------joints  ---------------------------------------------------
"""
Inner join is the default join in PySpark and it’s mostly used. This joins two datasets on key columns, where keys don’t 
match the rows get dropped from both datasets (emp & dept).

Outer a.k.a full, fullouter join returns all rows from both datasets, where join expression doesn’t match it returns null on respective record columns. 
Left a.k.a Leftouter join returns all rows from the left dataset regardless of match found on the right dataset when join expression doesn’t match, it assigns null for 
that record and drops records from right where match not found. 
Right a.k.a Rightouter join is opposite of left join, here it returns all rows from the right dataset regardless of math found on the left dataset, when join expression 
doesn’t match, it assigns null for that record and drops records from left where match not found.

leftsemi join is similar to inner join difference being leftsemi join returns all columns from the left dataset and ignores all columns from the right dataset. 
In other words, this join returns columns from the only left dataset for the records match in the right dataset on join expression, records not matched on join 
expression are ignored from both left and right datasets. 
leftanti join does the exact opposite of the leftsemi, leftanti join returns only columns from the left dataset for non-matched records.


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

emp = [(1,"Smith",-1,"2018","10","M",3000), \
    (2,"Rose",1,"2010","20","M",4000), \
    (3,"Williams",1,"2010","10","M",1000), \
    (4,"Jones",2,"2005","10","F",2000), \
    (5,"Brown",2,"2010","40","",-1), \
      (6,"Brown",2,"2010","50","",-1) \
  ]
empColumns = ["emp_id","name","superior_emp_id","year_joined", \
       "emp_dept_id","gender","salary"]

empDF = spark.createDataFrame(data=emp, schema = empColumns)
empDF.printSchema()
empDF.show(truncate=False)


dept = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) \
  ]
deptColumns = ["dept_name","dept_id"]
deptDF = spark.createDataFrame(data=dept, schema = deptColumns)
deptDF.printSchema()
deptDF.show(truncate=False)
  
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"inner") \
     .show(truncate=False)

empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"outer") \
    .show(truncate=False)
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"full") \
    .show(truncate=False)
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"fullouter") \
    .show(truncate=False)
    
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"left") \
    .show(truncate=False)
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"leftouter") \
   .show(truncate=False)

empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"right") \
   .show(truncate=False)
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"rightouter") \
   .show(truncate=False)

empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"leftsemi") \
   .show(truncate=False)
   
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"leftanti") \
   .show(truncate=False)
   
empDF.alias("emp1").join(empDF.alias("emp2"), \
    col("emp1.superior_emp_id") == col("emp2.emp_id"),"inner") \
    .select(col("emp1.emp_id"),col("emp1.name"), \
      col("emp2.emp_id").alias("superior_emp_id"), \
      col("emp2.name").alias("superior_emp_name")) \
   .show(truncate=False)

empDF.createOrReplaceTempView("EMP")
deptDF.createOrReplaceTempView("DEPT")
   
joinDF = spark.sql("select * from EMP e, DEPT d where e.emp_dept_id == d.dept_id") \
  .show(truncate=False)

joinDF2 = spark.sql("select * from EMP e INNER JOIN DEPT d ON e.emp_dept_id == d.dept_id") \
  .show(truncate=False)

df1.join(df2,df1.id1 == df2.id2,"inner") \
   .join(df3,df1.id1 == df3.id3,"inner")
   
"""

#-------------------------------------------------------- union ,union all,unionByName ------------------------------
#Dataframe union() – union() method of the DataFrame is used to merge two DataFrame’s of the same structure/schema. If schemas are not the same it returns an error.
unionDF = df.union(df2)
unionDF.show(truncate=False)

unionAllDF = df.unionAll(df2)
unionAllDF.show(truncate=False)

disDF = df.union(df2).distinct()
disDF.show(truncate=False)

merged_df = df1.unionByName(df2, allowMissingColumns=True)
# merged_df.show()


#------------------MAP RDD/DF
"""PySpark map (map()) is an RDD transformation that is used to apply the transformation function (lambda) on every element of RDD/DataFrame and returns a new RDD.
RDD map() transformation is used to apply any complex operations like adding a column, updating a column, transforming the data e.t.c, the output of map transformations 
would always have the same number of records as input.
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

data = [('James','Smith','M',30),
  ('Anna','Rose','F',41),
  ('Robert','Williams','M',62), 
]

columns = ["firstname","lastname","gender","salary"]
df = spark.createDataFrame(data=data, schema = columns)
rdd2=df.rdd.map(lambda x: 
    (x[0]+","+x[1],x[2],x[3]*2)
    )  
df2=rdd2.toDF(["name","gender","new_salary"]   )
df2.show()
"""


# flatmap, explode
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

data = ["Project Gutenberg’s",
        "Alice’s Adventures in Wonderland",
        "Project Gutenberg’s",
        "Adventures in Wonderland",
        "Project Gutenberg’s"]
rdd=spark.sparkContext.parallelize(data)
for element in rdd.collect():
    print(element)

#Flatmap
rdd2=rdd.flatMap(lambda x: x.split(" "))
for element in rdd2.collect():
    print(element)

arrayData = [
('James', ['Java', 'Scala'], {'hair': 'black', 'eye': 'brown'}),
('Michael', ['Spark', 'Java', None], {'hair': 'brown', 'eye': None}),
('Robert', ['CSharp', ''], {'hair': 'red', 'eye': ''}),
('Washington', None, None),
('Jefferson', ['1', '2'], {})
df = spark.createDataFrame(data=arrayData, schema=['name', 'knownLanguages', 'properties'])

from pyspark.sql.functions import explode

df2 = df.select(df.name, explode(df.knownLanguages))
df2.printSchema()
df2.show()


#---------------- fill, fillna---------------
df.fillna(value=0).show()
df.fillna(value=0,subset=["population"]).show()
df.na.fill(value=0).show()
df.na.fill(value=0,subset=["population"]).show()

df.fillna(value="").show()
df.na.fill(value="").show()

df.fillna("unknown",["city"]) \
    .fillna("",["type"]).show()

df.fillna({"city": "unknown", "type": ""}) \
    .show()

df.na.fill("unknown",["city"]) \
    .na.fill("",["type"]).show()

df.na.fill({"city": "unknown", "type": ""}) \
    .show()

#--------------foreach,foreach partitions

from pyspark.sql.functions import concat_ws,col,lit
df.select(concat_ws(",",df.firstname,df.lastname).alias("name"), \
          df.gender,lit(df.salary*2).alias("new_salary")).show()

df.foreach(lambda x:
    print("Data ==>"+x["firstname"]+","+x["lastname"]+","+x["gender"]+","+str(x["salary"]*2))
    )

#---------------------pivot

df.groupBy("Product").pivot("Country").sum("Amount")
pivotDF.printSchema()
pivotDF.show(truncate=False)

from pyspark.sql.functions import explode
df.select(df.name,explode(df.languagesAtSchool)).show()

from pyspark.sql.functions import split
df.select(split(df.name,",").alias("nameAsArray")).show()
from pyspark.sql.functions import array
df.select(df.name,array(df.currentState,df.previousState).alias("States")).show()

from pyspark.sql.functions import array_contains
df.select(df.name,array_contains(df.languagesAtSchool,"Java")
    .alias("array_contains")).show()


#------------------------------map type
f3=df.rdd.map(lambda x: \
    (x.name,x.properties["hair"],x.properties["eye"])) \
    .toDF(["name","hair","eye"])
df3.printSchema()
df3.show()

df.withColumn("hair",df.properties["hair"]) \
  .withColumn("eye",df.properties["eye"]) \
  .drop("properties") \
  .show()

from pyspark.sql.functions import map_keys
df.select(df.name,map_keys(df.properties)).show()
