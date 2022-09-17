import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
from pyspark.sql.functions import *

# read the transaction_history.csv file
py_spark_data_frame = spark.read.csv("transaction_history.csv", inferSchema=True, header=True)

# PRINT THE SCHEMA OF THE DATASET
py_spark_data_frame.printSchema()

# COUNT OF TRANSACTION
py_spark_data_frame.count()

# UNIQUE ACCOUNT NO COUNT
py_spark_data_frame.select('Account No').distinct().count()

# COUNT OF TRANSACTION IN AN EVERY ACCOUNT
py_spark_data_frame.groupBy('Account No').count().show()

# MAXIMUM WITHDRAWAL AMOUNT IN AN EVERY ACCOUNT
py_spark_data_frame.groupBy('Account No').max('WITHDRAWAL AMT').orderBy(max(' WITHDRAWAL AMT '),
                                                                        ascending=False).show()

# MINIMUM WITHDRAWAL AMOUNT IN AN EVERY ACCOUNT
py_spark_data_frame.groupBy('Account No').min('WITHDRAWAL AMT').orderBy(min(' WITHDRAWAL AMT '),
                                                                        ascending=False).show()


# MAXIMUM DEPOSIT AMOUNT IN AN EVERY ACCOUNT
py_spark_data_frame.groupBy('Account No').max('DEPOSIT AMT').show()

# MINIMUM DEPOSIT AMOUNT IN AN EVERY ACCOUNT
py_spark_data_frame.groupBy('Account No').min('DEPOSIT AMT').show()

# SUM OF BALANCE OF EVERY ACCOUNT
py_spark_data_frame.groupBy('Account No').sum('BALANCE AMT').show()

# NUMBER OF TRANSACTION ON EACH DAY
py_spark_data_frame.groupBy('VALUE DATE').count().orderBy('count', ascending=False).show()

# Get accounts Number where WITHDRAWAL AMT IS GREATER THAN 100000000
py_spark_data_frame.select('Account No').filter(py_spark_data_frame[' WITHDRAWAL AMT '] > 100000000).show()