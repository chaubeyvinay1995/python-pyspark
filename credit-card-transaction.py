import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
from pyspark.sql.functions import *

py_spark_data_frame = pyspark.read.csv('credit-card.csv', header=True, inferSchema=True)

# printSchema, to print the schema i.e. headers information of the
py_spark_data_frame.printSchema()

# Get Unique CustomerId count
py_spark_data_frame.groupBy('Customer Id').count().orderBy('count').count()

# get the detail of the customer whose IsActiveMember > 0
py_spark_data_frame.select('CustomerId', 'Surname', 'IsActiveMember'). \
    filter(py_spark_data_frame['IsActiveMember'] > 0).show()

# get the length of the columns
len(py_spark_data_frame.columns)

# get the distinct count
py_spark_data_frame.distinct().count()

# get the customer whose credit card is score is greater than or equal to 750
py_spark_data_frame.select('Surname', 'CustomerId', 'CreditScore', 'Gender', 'Geography', 'Age', 'Tenure',
                           'Balance', 'EstimatedSalary', 'Exited'). \
    filter(py_spark_data_frame['CreditScore'] >= 750).show()

# get the customer whose credit card is score is greater than or equal to 750 and is Active member
py_spark_data_frame.select('CustomerId', 'Surname', 'IsActiveMember').filter(
    py_spark_data_frame['IsActiveMember'] > 0 and py_spark_data_frame['CreditScore'] >= 750).show()

# get the customer who is exited for the Credit-Card
py_spark_data_frame.select('Surname', 'CustomerId', 'CreditScore', 'Gender',
                           'Geography', 'Age', 'Tenure').filter(py_spark_data_frame['Exited'] >= 1).show()

# get the credit card users in Spain
py_spark_data_frame.filter(py_spark_data_frame['Geography'] == "Spain").count()
py_spark_data_frame.filter(py_spark_data_frame['Geography'] == "Spain").show()

# get the credit card users whose is Active member and Credit card score is 750
py_spark_data_frame.filter((py_spark_data_frame['IsActiveMember'] >= 1)
                           & (py_spark_data_frame['CreditScore'] >= 750)).show()

# count the credit card user count whose EstimatedSalary is greater than 1000000 and is Existed for credit card.
py_spark_data_frame.filter((py_spark_data_frame['EstimatedSalary'] > 100000) &
                           (py_spark_data_frame['Exited'] >= 1)).count()

