#SQL functions
#date and timestamp function

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create SparkSession
spark = SparkSession.builder \
            .appName('SparkByExamples.com') \
            .getOrCreate()
data=[["1","2020-02-01"],["2","2019-03-01"],["3","2021-03-01"]]
df=spark.createDataFrame(data,["id","input"])
df.show()

#current date
df.select(current_date().alias("current_date")
  ).show(1)

#date_format()
df.select(col("input"), 
    date_format(col("input"), "MM-dd-yyyy").alias("date_format") 
  ).show()

#to_date
df.select(col("input"), 
    to_date(col("input"), "yyy-MM-dd").alias("to_date") 
  ).show()

#datediff()
df.select(col("input"), 
    datediff(current_date(),col("input")).alias("datediff")  
  ).show()

#months_between()
df.select(col("input"), 
    months_between(current_date(),col("input")).alias("months_between")  
  ).show()

#trunc()
df.select(col("input"), 
    trunc(col("input"),"Month").alias("Month_Trunc"), 
    trunc(col("input"),"Year").alias("Month_Year"), 
    trunc(col("input"),"Month").alias("Month_Trunc")
   ).show()

#add_months() , date_add(), date_sub()
df.select(col("input"), 
    add_months(col("input"),3).alias("add_months"), 
    add_months(col("input"),-3).alias("sub_months"), 
    date_add(col("input"),4).alias("date_add"), 
    date_sub(col("input"),4).alias("date_sub") 
  ).show()

#year(), month(), month(),next_day(), weekofyear()
df.select(col("input"), 
     year(col("input")).alias("year"), 
     month(col("input")).alias("month"), 
     next_day(col("input"),"Sunday").alias("next_day"), 
     weekofyear(col("input")).alias("weekofyear") 
  ).show()

#dayofweek(), dayofmonth(), dayofyear()
df.select(col("input"),  
     dayofweek(col("input")).alias("dayofweek"), 
     dayofmonth(col("input")).alias("dayofmonth"), 
     dayofyear(col("input")).alias("dayofyear"), 
  ).show()

#current_timestamp()
data=[["1","02-01-2020 11 01 19 06"],["2","03-01-2019 12 01 19 406"],["3","03-01-2021 12 01 19 406"]]
df2=spark.createDataFrame(data,["id","input"])
df2.show(truncate=False)

#current_timestamp()
df2.select(current_timestamp().alias("current_timestamp")
  ).show(1,truncate=False)

#to_timestamp()
df2.select(col("input"), 
    to_timestamp(col("input"), "MM-dd-yyyy HH mm ss SSS").alias("to_timestamp") 
  ).show(truncate=False)

#hour, minute,second
data=[["1","2020-02-01 11:01:19.06"],["2","2019-03-01 12:01:19.406"],["3","2021-03-01 12:01:19.406"]]
df3=spark.createDataFrame(data,["id","input"])

df3.select(col("input"), 
    hour(col("input")).alias("hour"), 
    minute(col("input")).alias("minute"),
    second(col("input")).alias("second") 
  ).show(truncate=False)

# Array functions	
df = spark.createDataFrame([['A', 'a', '1'], ['B', 'b', '2'], ['C', 'c', '3']], ['col1', 'col2', 'col3'])
df.show()

from pyspark.sql import functions as F
# Assign label to PySpark column returned by array(~) using alias(~)
df.select(F.array('col1','col2','col3').alias('combined_col')).show()

df.select(F.array(F.col('col1'),df['col2'],'col3').alias('combined_col')).show()

# Creating a Dataframe for demonstration

# Import the SparkSession, Row
from pyspark.sql import SparkSession, Row

# Create a spark session using getOrCreate() function
spark_session = SparkSession.builder.getOrCreate()

# Create a spark context
sc = spark_session.sparkContext

# Create a data frame with columns 'Roll_Number', 'Full_Name',
# 'Marks' and 'Subjects'
df = sc.parallelize([Row(Roll_Number=1,
						Full_Name=['Arun', 'Kumar', 'Chaudhary'],
						Marks=[95, 58, 63],
						Subjects=['Maths', 'Physics', 'Chemistry']),
					Row(Roll_Number=2,
						Full_Name=['Aniket', 'Singh', 'Rajpoot'],
						Marks=[87, 69, 56],
						Subjects=['History', 'Geography', 'Arts']),
					Row(Roll_Number=3,
						Full_Name=['Ishita', 'Rai', 'Pundir'],
						Marks=[49, 75, 98],
						Subjects=['Accounts', 'Business Studies',
								'Maths'])]).toDF()

# Display the data frame
df.show(truncate=False)

# Apply function to all values in array column in PySpark

# Import the UDF, ArrayType, IntegerType
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, IntegerType

# Reduce each marks by 3
reduce_marks = udf(lambda x: [i-3 for i in x], ArrayType(IntegerType()))

# Add a new column to display updated marks in data frame
updated_df = df.withColumn('Updated Marks', reduce_marks('Marks'))

# Display the updated data frame
updated_df.show(truncate=False)

# Apply function to all values in array column in PySpark

# Import the UDF, ArrayType, IntegerType
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType

# Create a UDF to convert each array element to upper case
upper_case = udf(lambda x: [i.upper() for i in x],
				ArrayType(StringType()))

# Add a new column to display updated full name in data frame
updated_df = df.withColumn('Updated_Full_Name', upper_case('Full_Name'))

# Display the updated data frame
updated_df.show(truncate=False)

# #aggregate functions
#https://sparkbyexamples.com/pyspark/pyspark-aggregate-functions/

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import approx_count_distinct,collect_list
from pyspark.sql.functions import collect_set,sum,avg,max,countDistinct,count
from pyspark.sql.functions import first, last, kurtosis, min, mean, skewness 
from pyspark.sql.functions import stddev, stddev_samp, stddev_pop, sumDistinct
from pyspark.sql.functions import variance,var_samp,  var_pop

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

simpleData = [("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)
  ]
schema = ["employee_name", "department", "salary"]
  
df = spark.createDataFrame(data=simpleData, schema = schema)
df.printSchema()
df.show(truncate=False)

print("approx_count_distinct: " + \
      str(df.select(approx_count_distinct("salary")).collect()[0][0]))

print("avg: " + str(df.select(avg("salary")).collect()[0][0]))

df.select(collect_list("salary")).show(truncate=False)

df.select(collect_set("salary")).show(truncate=False)

df2 = df.select(countDistinct("department", "salary"))
df2.show(truncate=False)
print("Distinct Count of Department & Salary: "+str(df2.collect()[0][0]))

print("count: "+str(df.select(count("salary")).collect()[0]))
df.select(first("salary")).show(truncate=False)
df.select(last("salary")).show(truncate=False)
df.select(kurtosis("salary")).show(truncate=False)
df.select(max("salary")).show(truncate=False)
df.select(min("salary")).show(truncate=False)
df.select(mean("salary")).show(truncate=False)
df.select(skewness("salary")).show(truncate=False)
df.select(stddev("salary"), stddev_samp("salary"), \
    stddev_pop("salary")).show(truncate=False)
df.select(sum("salary")).show(truncate=False)
df.select(sumDistinct("salary")).show(truncate=False)
df.select(variance("salary"),var_samp("salary"),var_pop("salary")) \
  .show(truncate=False)

#windows function
#1.Ranking Function
#2.Analytic Function
#3.Aggregate Function

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

simpleData = (("James", "Sales", 3000), \
    ("Michael", "Sales", 4600),  \
    ("Robert", "Sales", 4100),   \
    ("Maria", "Finance", 3000),  \
    ("James", "Sales", 3000),    \
    ("Scott", "Finance", 3300),  \
    ("Jen", "Finance", 3900),    \
    ("Jeff", "Marketing", 3000), \
    ("Kumar", "Marketing", 2000),\
    ("Saif", "Sales", 4100) \
  )
 
columns= ["employee_name", "department", "salary"]
df = spark.createDataFrame(data = simpleData, schema = columns)
df.printSchema()
df.show(truncate=False)

#window ranking function
#row number
#sequential row number starting from 1 to the result of each window partition.

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
windowSpec  = Window.partitionBy("department").orderBy("salary")

df.withColumn("row_number",row_number().over(windowSpec)) \
    .show(truncate=False)

#rank window function
#window function is used to provide a rank to the result within a window partition. 
#This function leaves gaps in rank when there are ties.

from pyspark.sql.functions import rank
df.withColumn("rank",rank().over(windowSpec)) \
    .show()

# Dense rank window function
#window function is used to get the result with rank of rows within a window partition without any gaps. 
#This is similar to rank() function difference being rank function leaves gaps in rank when there are ties.

from pyspark.sql.functions import dense_rank
df.withColumn("dense_rank",dense_rank().over(windowSpec)) \
    .show()

#percent rank window function
from pyspark.sql.functions import percent_rank
df.withColumn("percent_rank",percent_rank().over(windowSpec)) \
    .show()
    
#ntile window function
from pyspark.sql.functions import ntile
df.withColumn("ntile",ntile(2).over(windowSpec)) \
    .show()

#PySpark window analytic function

#cume_dist : cumulative distibuion
from pyspark.sql.functions import cume_dist
df.withColumn("cum_dist",cume_dist().over(windowSpec)) \
    .show()

#lag window function
from pyspark.sql.functions import lag
df.withColumn("lag",lag("salary",2).over(windowSpec)) \
    .show()

#lead window function
from pyspark.sql.functions import lead
df.withColumn("lead",lead("salary",2).over(windowSpec)) \
    .show()   

#window aggregate function

windowSpecAgg  = Window.partitionBy("department")
from pyspark.sql.functions import col,avg,sum,min,max,row_number 
df.withColumn("row",row_number().over(windowSpec)) \
  .withColumn("avg", avg(col("salary")).over(windowSpecAgg)) \
  .withColumn("sum", sum(col("salary")).over(windowSpecAgg)) \
  .withColumn("min", min(col("salary")).over(windowSpecAgg)) \
  .withColumn("max", max(col("salary")).over(windowSpecAgg)) \
  .where(col("row")==1).select("department","avg","sum","min","max") \
  .show()

#JSON function

from pyspark.sql import SparkSession,Row
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

jsonString="""{"Zipcode":704,"ZipCodeType":"STANDARD","City":"PARC PARQUE","State":"PR"}"""
df=spark.createDataFrame([(1, jsonString)],["id","value"])
df.show(truncate=False)

#Convert JSON string column to Map type
from pyspark.sql.types import MapType,StringType
from pyspark.sql.functions import from_json
df2=df.withColumn("value",from_json(df.value,MapType(StringType(),StringType())))
df2.printSchema()
df2.show(truncate=False)

#to_json
from pyspark.sql.functions import to_json,col
df2.withColumn("value",to_json(col("value"))) \
   .show(truncate=False)

#json_tuple
from pyspark.sql.functions import json_tuple
df.select(col("id"),json_tuple(col("value"),"Zipcode","ZipCodeType","City")) \
    .toDF("id","Zipcode","ZipCodeType","City") \
    .show(truncate=False)

#get_json_object
from pyspark.sql.functions import get_json_object
df.select(col("id"),get_json_object(col("value"),"$.ZipCodeType").alias("ZipCodeType")) \
    .show(truncate=False)

#schema_of_json
from pyspark.sql.functions import schema_of_json,lit
schemaStr=spark.range(1) \
    .select(schema_of_json(lit("""{"Zipcode":704,"ZipCodeType":"STANDARD","City":"PARC PARQUE","State":"PR"}"""))) \
    .collect()[0][0]
print(schemaStr)

# Data Source API	

# Read & Write Parquest file	
import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("parquetFile").getOrCreate()
data =[("James ","","Smith","36636","M",3000),
              ("Michael ","Rose","","40288","M",4000),
              ("Robert ","","Williams","42114","M",4000),
              ("Maria ","Anne","Jones","39192","F",4000),
              ("Jen","Mary","Brown","","F",-1)]
columns=["firstname","middlename","lastname","dob","gender","salary"]
df=spark.createDataFrame(data,columns)
df.write.mode("overwrite").parquet("/tmp/output/people.parquet")
parDF1=spark.read.parquet("/tmp/output/people.parquet")
parDF1.createOrReplaceTempView("parquetTable")
parDF1.printSchema()
parDF1.show(truncate=False)

parkSQL = spark.sql("select * from ParquetTable where salary >= 4000 ")
parkSQL.show(truncate=False)

spark.sql("CREATE TEMPORARY VIEW PERSON USING parquet OPTIONS (path \"/tmp/output/people.parquet\")")
spark.sql("SELECT * FROM PERSON").show()

df.write.partitionBy("gender","salary").mode("overwrite").parquet("/tmp/output/people2.parquet")

parDF2=spark.read.parquet("/tmp/output/people2.parquet/gender=M")
parDF2.show(truncate=False)

spark.sql("CREATE TEMPORARY VIEW PERSON2 USING parquet OPTIONS (path \"/tmp/output/people2.parquet/gender=F\")")
spark.sql("SELECT * FROM PERSON2" ).show()

#avro file
#avro-example.py
from pyspark.sql import SparkSession

appName = "PySpark Example - Read and Write Avro"
master = "local"

# Create Spark session
spark = SparkSession.builder \
    .appName(appName) \
    .master(master) \
    .getOrCreate()

# List
data = [{
    'col1': 'Category A',
    'col2': 100
}, {
    'col1': 'Category B',
    'col2': 200
}, {
    'col1': 'Category C',
    'col2': 300
}]

df = spark.createDataFrame(data)
df.show()

#Data source API

#Read & Write Parquest file	
#Read & Write AVRO file
#Read & Write HBase using “hbase-spark” Connector	
#Read & Write from HBase using Hortonworks	
#Read & Write ORC file
#Read binary 	


# Streaming & Kafka	

#reading data from kafka
# Subscribe to 1 topic
# df = spark \
#   .readStream \
#   .format("kafka") \
#   .option("kafka.bootstrap.servers", "host1:port1,host2:port2") \
#   .option("subscribe", "topic1") \
#   .load()
# df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# # Subscribe to 1 topic, with headers
# df = spark \
#   .readStream \
#   .format("kafka") \
#   .option("kafka.bootstrap.servers", "host1:port1,host2:port2") \
#   .option("subscribe", "topic1") \
#   .option("includeHeaders", "true") \
#   .load()
# df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "headers")

# # Subscribe to multiple topics
# df = spark \
#   .readStream \
#   .format("kafka") \
#   .option("kafka.bootstrap.servers", "host1:port1,host2:port2") \
#   .option("subscribe", "topic1,topic2") \
#   .load()
# df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# # Subscribe to a pattern
# df = spark \
#   .readStream \
#   .format("kafka") \
#   .option("kafka.bootstrap.servers", "host1:port1,host2:port2") \
#   .option("subscribePattern", "topic.*") \
#   .load()
# df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
