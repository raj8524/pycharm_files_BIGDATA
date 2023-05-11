
"""
Question 21
-- Table: ActorDirector

-- +-------------+---------+
-- | Column Name | Type    |
-- +-------------+---------+
-- | actor_id    | int     |
-- | director_id | int     |
-- | timestamp   | int     |
-- +-------------+---------+
-- timestamp is the primary key column for this table.
 

-- Write a SQL query for a report that provides the pairs (actor_id, director_id) where the actor have cooperated with the director at least 3 times.

-- Example:

-- ActorDirector table:
-- +-------------+-------------+-------------+
-- | actor_id    | director_id | timestamp   |
-- +-------------+-------------+-------------+
-- | 1           | 1           | 0           |
-- | 1           | 1           | 1           |
-- | 1           | 1           | 2           |
-- | 1           | 2           | 3           |
-- | 1           | 2           | 4           |
-- | 2           | 1           | 5           |
-- | 2           | 1           | 6           |
-- +-------------+-------------+-------------+

-- Result table:
-- +-------------+-------------+
-- | actor_id    | director_id |
-- +-------------+-------------+
-- | 1           | 1           |
-- +-------------+-------------+
-- The only pair is (1, 1) where they cooperated exactly 3 times.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Window
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
data = [(1,1,0),(1,1,1),(1,1,2),(1,2,3),(1,2,4),(2,2,5),(2,1,6)]
columns = ["actor_id","dir_id","timestamp"]
df = spark.createDataFrame(data = data, schema = columns)
df.createOrReplaceTempView("actor")
spark.sql("select actor_id,dir_id from actor group by actor_id,dir_id having count(1)>=3 and count(2)>=3").show()
spark.sql("select actor_id,dir_id from actor group by actor_id,dir_id").show()
# spark.sql("select actor_id,dir_id from (select actor_id,dir_id,dense_rank() over(order by actor_id,dir_id) dense "
#           "group by actor_id,dir_id)b where dense=3").show()
df1=Window.orderBy("actor_id","dir_id")
df2=df.withColumn("dense",dense_rank().over(df1)).groupBy("actor_id","dir_id").count().filter(col("count")>=3)
df2.show()
df2=df.withColumn("dense",dense_rank().over(df1)).groupBy("actor_id","dir_id").count()\
    .selectExpr("actor_id","dir_id").where(col("count")>=3).show()



# df.show(truncate=False)