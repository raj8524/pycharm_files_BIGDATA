# this will have all necessary functions
from pyspark.sql.types import StructType,StructField,IntegerType,DoubleType,StringType,TimestampType

def read_Schema(schema_arg):
    split_values=schema_arg.split(",")
    d_type={"IntegerType()":IntegerType(),
            "StringType()": StringType(),
            "DoubleType()": DoubleType(),
            "TimestampType()": TimestampType(),
            }
    sch=StructType()
    for i in split_values:
        x=i.split(" ")
        sch.add(x[0],d_type[x[1]],True)
    return sch