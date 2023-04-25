#Create a dataframe using SQL concept, add a new column and compare 2 columns which give you output of minimum value.

#import libraries
from pyspark.sql import SparkSession
from pyspark.sql.types import *

#create sparksession
spark = SparkSession \
    .builder \
    .appName("how to read csv file") \
    .getOrCreate()

#read CSV
df = spark.read.csv('/home/devanshi/Desktop/workspace/PySpark/paydata.csv',header=True,inferSchema='True')
df.show()

# query
df.createOrReplaceTempView("paydata")
spark.sql("select ID, Name, basepay , overpay , Case When basepay > overpay Then overpay When basepay < overpay Then basepay Else 0 End As MinPay from paydata").show()


 