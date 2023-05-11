from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('Basics').getOrCreate()
df=spark.read.csv('D:\software_installation\Pycharm_files\Sample2.csv',inferSchema=True,header=True)
# df.printSchema()
# df.show()
df.head(3)[0]
# df.filter('share > 10000').show() # will give all column details if share is >10000
# df.filter('share > 10000').select('share_price').show() # will give share_price column details if share is >10000
# df.filter('share > 10000').select(['share_price','share_num']).show() # will give share_price,share_num column details if share is >10000
# df.filter(df['share'] > 10000).select(['share_price','share_num']).show() # will give share_price,share_num column details if share is >10000
# df.filter((df['share'] > 9500) & (df['share'] < 10000) ).select(['share_price','share_num']).show()
results=df.filter((df['share'] > 9500) & (df['share'] < 10000) ).select(['share_price','share_num']).collect()
results   # will be list of values
results[2] # will show 2nd element of the list
row=results[3]
row.asDict()

# groupBy and aggregate on csv file

# from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('Basics').getOrCreate()
df=spark.read.csv('D:\software_installation\Pycharm_files\Sample2.csv',inferSchema=True,header=True)
df.groupby('company').mean()  # will give all mean related details of the company
df.groupBy('company').mean().show() # will show all mean

df.agg({'share_num':'max'}).show()
df.agg({'share_price':'max'}).show()
df.agg({'share_price':'avg'}).show()
from pyspark.sql.functions import countDistinct,avg,stddev,format_number
df.select(avg('share_price')).show()
df.select(avg('share_price')).alias('average_sales').show()
df.select(stddev('share_price')).show()
std_dev_price=df.select(stddev('share_price')).alias('average_sales')
std_dev_price.select(format_number('average_sales',2)).show()
df.orderBy('share_price').show()
df.orderBy(df['share_price'].desc()).show(



data_group=df.groupBy('company')
data_group.agg({'share_price':'max'}).show()


#-----------------missing data---------------in csv file

# spark=SparkSession.builder.appName('Basics').getOrCreate()
df=spark.read.csv('D:\software_installation\Pycharm_files\Sample2.csv',inferSchema=True,header=True)
df.na.drop().show()    # to drop null values row completely
df.na.drop(thresh=2).show()# will drop null rows if two vales r null else it will dispaly.
df.na.drop(how='any').show()  # will drop rows if any null is found
df.na.drop(how='all').show()  # if all rows r null,then drop it
df.na.drop(subset=['share_num','share_price']).show()  # will drop rows value if in share_num,share_price is null rest will display
df.na.fill(0).show()  # will fill 0 in numeric type of column if its value is null.
df.na.fill('FILL VALUE').show()   # will fill FILL VALUE string  in string type of column if its value is null.
df.na.fill(0,subset=['share_price','share_num']).show()  # will fill 0 in share_price,share_num column if its value is null.

#--------to fill missing numeric data with mean value
from pyspark.sql.functions import mean
mean_data=df.select(mean(df['share_price'])).collect()
mean_data[0] # will show its Row(avg(share_price)=313.20031249999994)
mean_value=mean_data[0][0] #313.20031249999994
df.na.fill(mean_value,subset=['share_price','share_num']).show()  # will fill na rows values with mean_value
df.na.fill(df.select(mean(df['share_price'])).collect()[0][0],subset=['share_price','share_num']).show()

#----------------date functions-------------
from pyspark.sql.functions import dayofyear,year,weekofyear,month
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('Basics').getOrCreate()
df=spark.read.csv('D:\software_installation\Pycharm_files\Sample2.csv',inferSchema=True,header=True)
df.select(dayofyear(df['date'])).show()
df.select(year(df['date'])).show()       #date,year is the column of the csv file
newdf=df.withcolumn('year1',year(df['date'])).show()
newdf.groupBy('year').mean().show()
results=newdf.groupBy('year').mean().select(['year',avg("closing-price")]).show()
results.withColumnRenamed('avg(close)','averageclosingprice').show()
new1=results.withColumnRenamed('avg(close)','averageclosingprice')
new1.select('year',format_number('averageclosingprice',2).alias('avg-close')).show()


for row in df.head(5):
    print(row)
    print('\n')


"""
linear regression-least square method
minimizing the sum of squares of the residuals (vertical points with the fitted line)
"""
