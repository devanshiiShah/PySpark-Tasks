# Useful function for PySpark dataframe:
#importing pyspark
import pyspark

#importing sparksessio
from pyspark.sql import SparkSession

#for split
from pyspark.sql.functions import split

#for lit
from pyspark.sql.functions import lit,col,when
#creating a sparksession object and providing appName 
spark=SparkSession.builder.appName("pysparkdf").getOrCreate()

#read CSV
df = spark.read.csv('/home/devanshi/Desktop/workspace/PySpark/cereal.csv',header=True,inferSchema='True')
df.printSchema()

#select
df.select('name', 'mfr', 'rating').show(10)

#with column
df.withColumn("Calories",df['calories'].cast("Integer")).printSchema()

#groupBy
df.groupBy("name", "calories").count().show()

#orderBy
df.orderBy("fat").show()

#Split
df1 = df.withColumn('Name1', split(df['name'], " ").getItem(0)).withColumn('Name2', split(df['name'], " ").getItem(1))
df1.select("name", "Name1", "Name2").show()

#lit
df2 = df.select(col("name"),lit("75 gm").alias("intake quantity"))
df2.show()

#when
df.select("name", when(df.vitamins >= "25", "rich in vitamins")).show()
df.show()

#filter
df.filter(df.calories == "100").show()

#isNull
# df.filter(df.name.isNull()).show()