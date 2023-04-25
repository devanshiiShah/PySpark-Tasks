#final task

from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import *
from pyspark.sql.functions import to_date
from pyspark.sql.window import Window

from pyspark.sql.functions import col, row_number
spark = SparkSession.builder.master("local[1]") \
                    .appName('SparkByExamples.com') \
                    .getOrCreate()

#Read Parquet File 
df = spark.read.parquet("/home/devanshi/Desktop/workspace/pract/userdata1.parquet")
df.show()

#Process data
#Add Column "Full Name"
df = df.withColumn("full_name", concat_ws(",","first_name",'last_name'))
df.show
#drop first_name and last_name
df = df.drop('first_name','last_name')

#Remove Comment column
df = df.drop('comments')
# df.show()

#Add New Column "Registraion Date" from registration_dttm
df = df.withColumn("Registraion_Date", to_date(df.registration_dttm,"yyyy-MM-dd"))
# df = df.select(col("Registraion_Date"),to_date(col("registration_dttm"),"yyyy-MM-dd HH:mm:ss").alias("Registraion_Date")) 
df.show()

#zero format of date,Accurate formate of pyspark 
# df = df.select(F.col("birthdate"),F.to_date(F.col("birthdate"),"M/d/y").alias("date"))
# df.show()
df = df.withColumn("bdate",to_date(df.birthdate,"M/d/y"))
df.show()

df = df.withColumn('birth_year', F.year(df.bdate))
df.show()

df = df.withColumn('birth_month', F.month(df.bdate))
df.show()

df = df.withColumn('birth_day', F.dayofmonth(df.bdate))
df.show()

#Add Age column from birth-date
df = df.withColumn("age", F.floor(F.datediff(F.current_date(), F.col("bdate"))/365.25))
df.show()

#Store as csv in local
# read data from parquet
df = spark.read.parquet('/home/devanshi/Desktop/workspace/pract/userdata1.parquet',header=True)

#write data to csv
df.write.format("csv").save("/home/devanshi/Desktop/csv",header=True,inferSchema='True')

#Add Partition based on csv
#1) Male/Female
df.write.option("header",True) \
    .mode("overwrite")\
    .partitionBy("gender") \
    .csv("/home/devanshi/test")

#2) Year (From Birthdate)
#3) Month (From Birthdate)
#4) Day (From Birthdate)
df.write.option("header",True) \
    .mode("overwrite")\
    .partitionBy('birth_year','birth_month','birth_day') \
    .csv('/home/devanshi/partition')

#Business Date
#Find out those users who has salary>22000
df =df.filter(col("salary") > 22000)
df.write.format("csv").mode('overwrite').save("/home/devanshi/BDateSal")

#Find out those users who has age>60
df =df.filter(col("age") > 60)
df.write.format("csv").mode('overwrite').save("/home/devanshi/BDateAge")
  

