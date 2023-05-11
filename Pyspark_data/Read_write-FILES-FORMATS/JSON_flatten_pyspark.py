
IN_DIR = '/mnt/data/'
data={

    "sensorName": "snx001",
    "sensorDate": "2020-01-01",
    "sensorReadings": [
        {
            "sensorChannel": 1,
            "sensorReading": 3.7465084060850105,
            "datetime": "2020-01-01 00:00:00"
        },
        {
            "sensorChannel": 2,
            "sensorReading": 10.543041369293153,
            "datetime": "2020-01-01 00:00:00"
        }

    ]
}

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('Basics').getOrCreate()
from pyspark.sql.functions import explode
sensor_schema = StructType(fields=[
    StructField('sensorName', StringType(), False),
    StructField('sensorDate', StringType(), True),
    StructField(
        'sensorReadings', ArrayType(
            StructType([
                StructField('sensorChannel', IntegerType(), False),
                StructField('sensorReading', DoubleType(), True),
                StructField('datetime', StringType(), True)
            ])
        )
    )
])

data_df = spark.read.json(IN_DIR + '*.json', schema=sensor_schema)
data_df.show()

data_df = data_df.select(
    "sensorName",
    explode("sensorReadings").alias("sensorReadingsExplode")
).select("sensorName", "sensorReadingsExplode.*")