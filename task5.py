#create the script using pyspark that read data from csv and write to the PostgreSQL database.
# new data frame with split value columns and groupby quarter
# Imports
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pyspark
from pyspark.sql.functions import split
# Create SparkSession
spark = SparkSession \
    .builder \
    .appName("postgreSQL") \
    .config("spark.jars", "/home/devanshi/Downloads/postgresql-42.6.0.jar") \
    .getOrCreate()

#read data from CSV
df = spark.read.csv('/home/devanshi/Desktop/workspace/alcon/lensdata/ls3_qa_40_45_sample_size_202303231102.csv',header=True,inferSchema='True')
columns = df.columns
import random as rd
# columns1 = columns[:rd.random()]
columns1 = columns[:8]
df1 = df.drop(*columns1) 
#sample date quarterly
df = df.select(
    '*',
    F.weekofyear('sample_date').alias('week_year'), 
    F.quarter('sample_date').alias('quarter')
)
df.show()

# new data frame with split value columns
split_col = pyspark.sql.functions.split(df['sample_type'], '-')
df2 = df.withColumn('year', split_col.getItem(0)) \
       .withColumn('month', split_col.getItem(1)) \
       .withColumn('day', split_col.getItem(2))
df2.show(truncate=False) 

# #split
# df = spark.createDataFrame([('oneAtwoBthreeC',)], ['s',])
# df.select(split(df.s, '[ABC]', 2).alias('datatable1')).collect()

# df.select(split(df.s, '[ABC]', -1).alias('datatable2')).collect()

# #randomSplit
# splits = df.randomSplit([3.0,5.0])
# splits[1].count()
# df.show()

# df.columns
# #group by quarter
df.groupBy("quarter").count().show(truncate=False)

#write in postgresSQL
df.write.jdbc("jdbc:postgresql://localhost:5432/postgres?user=postgres&password=1234",'test',mode='overwrite',properties={
    'driver':'org.postgresql.Driver'
})
df1.write.jdbc("jdbc:postgresql://localhost:5432/postgres?user=postgres&password=1234",'test1',mode='overwrite',properties={
    'driver':'org.postgresql.Driver'
})

