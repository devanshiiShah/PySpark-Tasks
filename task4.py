#create the script using pyspark that read data from csv and write to the PostgreSQL database.
# Imports
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession \
    .builder \
    .appName("postgreSQL") \
    .config("spark.jars", "/home/devanshi/Downloads/postgresql-42.6.0.jar") \
    .getOrCreate()

#read data from CSV
df = spark.read.csv('/home/devanshi/Desktop/workspace/PySpark/paydata.csv',header=True,inferSchema='True')
df.show()

#write data to postgresSQL
df.write.jdbc("jdbc:postgresql://localhost:5432/postgres?user=postgres&password=1234",'test',mode='overwrite',properties={
    'driver':'org.postgresql.Driver'
})




