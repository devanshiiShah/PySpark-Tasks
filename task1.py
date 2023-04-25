# Create a dataframe using UDF concept, add a new column and compare 2 columns which give you output of minimum value.

# Import the libraries
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf, array
from pyspark.sql.types import StructType

# Create a spark session using getOrCreate() function
spark = SparkSession.builder .appName('SparkByExamples.com').getOrCreate()

# sample
min_cols = udf(lambda arr: min(arr), IntegerType())
spark.createDataFrame([(101, 15, 16)], ['ID', 'A', 'B']) \
    .show()
spark.createDataFrame([(101, 15, 16)], ['ID', 'A', 'B'])\
     .withColumn('Result', min_cols(array('A', 'B'))).show()

# Create a data frame
data = [('Devanshi','Shah','F',5000,6500), ('Smit','Shah','M',4000,2700),
  ('Disha','Shah','F',6000,4300)]
columns = ["firstname","lastname","gender","salary1","salary2"]
df = spark.createDataFrame(data=data, schema = columns)
df.show(truncate=False)

#Add new column which gets min value using UDF 
min_cols = udf(lambda arr: min(arr), IntegerType())
df = spark.createDataFrame(data=data, schema=columns)
df.withColumn('Min_salary',min_cols(array('salary1','salary2'))).show()

#CSV
#Add new column which gets min value using UDF 
min_cols = udf(lambda arr: min(arr), IntegerType())
spark = SparkSession.builder.appName('aggs').getOrCreate()
schema = StructType() 
df = spark.read.csv("/home/devanshi/Desktop/workspace/PySpark/paydata.csv",header=True,inferSchema='True')
df.show()

# function
# df = df.withColumn('min', least('basepay','overpay'))
# df.show()

#Add new column which gets min value using UDF Function
min_cols = udf(lambda arr: min(arr), IntegerType())
spark = SparkSession.builder.appName('aggs').getOrCreate()
df.createOrReplaceTempView('paydata')
df.withColumn('min_pay',min_cols(array('basepay','overpay'))).show()






