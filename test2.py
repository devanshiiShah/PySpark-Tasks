#Spark RDD
#pair RDD transformation
import pyspark

#import SparkSession for creating a session
from pyspark.sql import SparkSession

# import RDD from pyspark.rdd
from pyspark.rdd import RDD

#create an app named linuxhint
spark_app = SparkSession.builder.appName('linuxhint').getOrCreate()

# create 6 - subject and rating pairs
subjects_rating =spark_app.sparkContext.parallelize([('python',4),('javascript',2),('linux',5),('C#',4),('javascript',4),('python',3)])

#sortbykey
#apply sortByKey() transformation on the above subjects_rating to sort the keys in ascending order
print(subjects_rating.sortByKey().collect())

#groupbykey
#apply groupByKey() transformation on the above subjects_rating pair RDD
dictionary_rdd = subjects_rating.groupByKey().collect()

#get the keys and all values wrt to keys from the above dictionary rdd
for keys, values in dictionary_rdd:
    print(keys,"-->", list(values))

#reduceby    
print(subjects_rating.reduceByKey(lambda element1, element2: element1 * element2).collect())

#apply reduceByKey() transformation on the above subjects_rating to reduce Keys by dividing values with similar keys
print(subjects_rating.reduceByKey(lambda element1, element2: element1 / element2).collect())


#repartition and coalesce

# import pyspark
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName('SparkByExamples.com') \
#         .master("local[5]").getOrCreate()

# df = spark.range(0,20)
# print(df.rdd.getNumPartitions())

# spark.conf.set("spark.sql.shuffle.partitions", "500")

# rdd = spark.sparkContext.parallelize(range(0,20))
# print("From local[5]"+str(rdd.getNumPartitions()))

# rdd1 = spark.sparkContext.parallelize(range(0,25), 6)
# print("parallelize : "+str(rdd1.getNumPartitions()))

# """rddFromFile = spark.sparkContext.textFile("src/main/resources/test.txt",10)
# print("TextFile : "+str(rddFromFile.getNumPartitions())) """

# rdd1.saveAsTextFile("c://tmp/partition2")

# rdd2 = rdd1.repartition(4)
# print("Repartition size : "+str(rdd2.getNumPartitions()))
# rdd2.saveAsTextFile("c://tmp/re-partition2")

# rdd3 = rdd1.coalesce(4)
# print("Repartition size : "+str(rdd3.getNumPartitions()))
# rdd3.saveAsTextFile("c:/tmp/coalesce2")

#persistance Storage
from pyspark.sql import SparkSession
from pyspark import SparkConf, StorageLevel

app_name = "PySpark - persist() Example"
master = "local[8]"

conf = SparkConf().setAppName(app_name)\
    .setMaster(master)


spark = SparkSession.builder.config(conf=conf) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.createDataFrame([[1, 'A'], [2, 'B']], ['id', 'attr'])
df.persist(storageLevel=StorageLevel.MEMORY_ONLY)
print(df.count())
df.groupBy('id').count().show()
df.explain(mode='extended')

#broadcast

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

states = {"NY":"New York", "CA":"California", "FL":"Florida"}
broadcastStates = spark.sparkContext.broadcast(states)

data = [("Devanshi","Shah","USA","CA"),
    ("Smit","Shah","USA","NY"),
    ("Disha","Shah","USA","CA"),
    ("Kushal","Shah","USA","FL")
  ]

rdd = spark.sparkContext.parallelize(data)

def state_convert(code):
    return broadcastStates.value[code]

result = rdd.map(lambda x: (x[0],x[1],x[2],state_convert(x[3]))).collect()
print(result)

#create pyspark RDD
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
dept = [("Finance",10),("Marketing",20),("Sales",30),("IT",40)]
rdd = spark.sparkContext.parallelize(dept)

#convert into dataframe
df = rdd.toDF()
df.printSchema()
df.show(truncate=False)

deptColumns = ["dept_name","dept_id"]
df2 = rdd.toDF(deptColumns)
df2.printSchema()
df2.show(truncate=False)

#createDataFrame function
deptDF = spark.createDataFrame(rdd, schema = deptColumns)
deptDF.printSchema()
deptDF.show(truncate=False)

#Using createDataFrame() with StructType schema
from pyspark.sql.types import StructType,StructField, StringType
deptSchema = StructType([       
    StructField('dept_name', StringType(), True),
    StructField('dept_id', StringType(), True)
])

deptDF1 = spark.createDataFrame(rdd, schema = deptSchema)
deptDF1.printSchema()
deptDF1.show(truncate=False)

# Spark SQL	
# select single and multiple columns
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql import SparkSession
import findspark


findspark.init('c:/spark')

# Initialize our data
data2 = [("Pulkit", 12, "CS32", 82, "Programming"),
		("Ritika", 20, "CS32", 94, "Writing"),
		("Aakriti", 4, "BB21", 78, None),
		("Keshav", 18, None, 56, None)
		]

# Start spark session
spark = SparkSession.builder.appName("Student_Info").getOrCreate()

# Define schema
schema = StructType([
	StructField("Name", StringType(), True),
	StructField("Roll Number", IntegerType(), True),
	StructField("Class ID", StringType(), True),
	StructField("Marks", IntegerType(), True),
	StructField("Extracurricular", StringType(), True)
])

# read the dataframe
df = spark.createDataFrame(data=data2, schema=schema)

# slelct columns
df.select("Name", "Marks").show()

# stop the session
spark.stop()

#add a new column
# importing module
import pyspark

# import lit function
from pyspark.sql.functions import lit

# importing sparksession from pyspark.sql module
from pyspark.sql import SparkSession

# creating sparksession and giving an app name
spark = SparkSession.builder.appName('sparkdf').getOrCreate()

# list of employee data
data = [["1", "Dev", "company 1"],
		["2", "Ansh", "company 1"],
		["3", "Smit", "company 2"],
		["4", "Shru", "company 1"],
		["5", "pri", "company 1"]]

# specify column names
columns = ['ID', 'NAME', 'Company']

# creating a dataframe from the lists of data
dataframe = spark.createDataFrame(data, columns)

# Add a column named salary with value as 34000
dataframe.withColumn("salary", lit(34000)).show()

# Rename the column name
# Importing necessary libraries
from pyspark.sql import SparkSession

# Create a spark session
spark = SparkSession.builder.appName('pyspark - example join').getOrCreate()

# Create data in dataframe
data = [(('Ram'), '1991-04-01', 'M', 3000),
		(('sita'), '2000-05-19', 'M', 4000),
		(('meera'), '1978-09-05', 'M', 4000),
		(('Shyam'), '1967-12-01', 'F', 4000),
		(('radha'), '1980-02-17', 'F', 1200)]

# Column names in dataframe
columns = ["Name", "DOB", "Gender", "salary"]

# Create the spark dataframe
df = spark.createDataFrame(data=data,
						schema=columns)

# Print the dataframe
df.show()

# Rename the column name from DOB to DateOfBirth
# Print the dataframe
df.withColumnRenamed("DOB","DateOfBirth").show()
df.withColumnRenamed("Salary","stipend").show()

#drop columnName
# delete single column
dataframe = dataframe.drop('Salary')
dataframe.show()

# filter dataframe based on single condition
df2 = df.where(df.Gender == "M")
print(" After filter dataframe based on single condition  ")
df2.show()

#When otherwise
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
data = [("joy","M",60000),("jendya","M",70000),
        ("joeeily",None,400000),("jeng","F",500000),
        ("Jen","",None)]

columns = ["name","gender","salary"]
df = spark.createDataFrame(data = data, schema = columns)
df.show()

from pyspark.sql.functions import when
df2 = df.withColumn("new_gender", when(df.gender == "M","Male")
                                 .when(df.gender == "F","Female")
                                 .when(df.gender.isNull() ,"")
                                 .otherwise(df.gender))
df2.show()

# Collect data	
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

dept = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) \
  ]
deptColumns = ["dept_name","dept_id"]
deptDF = spark.createDataFrame(data=dept, schema = deptColumns)
deptDF.printSchema()
deptDF.show(truncate=False)

dataCollect = deptDF.collect()

print(dataCollect)

dataCollect2 = deptDF.select("dept_name").collect()
print(dataCollect2)

for row in dataCollect:
    print(row['dept_name'] + "," +str(row['dept_id']))

#Distinct 

# Import
from pyspark.sql import SparkSession

# Create SparrkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

# Prepare Data
data = [("James", "Sales", 3000), \
    ("jolly", "Sales", 4600), \
    ("Robert", "Sales", 4100), \
    ("James", "Sales", 3000)
  ]

# Create DataFrame
columns= ["employee_name", "department", "salary"]
df = spark.createDataFrame(data = data, schema = columns)
df.printSchema()
df.show(truncate=False)


# Select distinct rows
distinctDF = df.distinct()
distinctDF.show(truncate=False)

#Pivot

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
#Create spark session
data = [("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"), \
      ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"), \
      ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"), \
      ("Banana",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico")]

columns= ["Product","Amount","Country"]
df = spark.createDataFrame(data = data, schema = columns)
df.printSchema()
df.show(truncate=False)


pivotDF = df.groupBy("Product").pivot("Country").sum("Amount")
pivotDF.printSchema()
pivotDF.show(truncate=False)

#DataType

#Struct Type
data = [("James","","Smith","36","M",3000),
    ("Michael","Rose","","40","M",4000),
    ("Robert","","Williams","42","M",4000),
    ("Maria","Anne","Jones","39","F",4000),
    ("Jen","Mary","Brown","","F",-1)
  ]

schema = StructType([ 
    StructField("firstname",StringType(),True), 
    StructField("middlename",StringType(),True), 
    StructField("lastname",StringType(),True), 
    StructField("age", StringType(), True), 
    StructField("gender", StringType(), True), 
    StructField("salary", IntegerType(), True) 
  ])
 
df = spark.createDataFrame(data=data,schema=schema)
df.printSchema()
df.show(truncate=False)

#Schema
#Define the basic Schema
from pyspark.sql import Row
from pyspark.sql.types import *
rdd = spark.sparkContext.parallelize([
    Row(name='Allie', age=2),
    Row(name='Sara', age=33),
    Row(name='jenny', age=31)])
schema = schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), False)])
df = spark.createDataFrame(rdd, schema)
df.show()
df.printSchema()

#group by

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,sum,avg,max

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

simpleData = [("James","Sales","NY",90000,34,10000),
    ("mike","Sales","NY",86000,56,20000),
    ("ram","Sales","CA",81000,30,23000),
    ("Maria","Finance","CA",90000,24,23000),
    ("Raman","Finance","CA",99000,40,24000),
    ("Scott","Finance","NY",83000,36,19000),
    ("Jen","Finance","NY",79000,53,15000),
    ("Jeff","Marketing","CA",80000,25,18000),
    ("Kumar","Marketing","NY",91000,50,21000)
  ]

schema = ["employee_name","department","state","salary","age","bonus"]
df = spark.createDataFrame(data=simpleData, schema = schema)
df.printSchema()
df.show(truncate=False)

df.groupBy("department").sum("salary").show(truncate=False)

df.groupBy("department").count().show(truncate=False)


df.groupBy("department","state") \
    .sum("salary","bonus") \
   .show(truncate=False)

df.groupBy("department") \
    .agg(sum("salary").alias("sum_salary"), \
         avg("salary").alias("avg_salary"), \
         sum("bonus").alias("sum_bonus"), \
         max("bonus").alias("max_bonus") \
     ) \
    .show(truncate=False)
    
df.groupBy("department") \
    .agg(sum("salary").alias("sum_salary"), \
      avg("salary").alias("avg_salary"), \
      sum("bonus").alias("sum_bonus"), \
      max("bonus").alias("max_bonus")) \
    .where(col("sum_bonus") >= 50000) \
    .show(truncate=False)

#Sort by
# Importing necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

# Create a spark session
spark = SparkSession.builder.appName(
'pyspark - example join').getOrCreate()

# Define data in a dataframe
dataframe = [
	("Sam", "Software Engineer", "IND", 10000),
	("Raj", "Data Scientist", "US", 41000),
	("Jonas", "Sales Person", "UK", 230000),
	("Peter", "CTO", "Ireland", 50000),
	("Hola", "Data Analyst", "Australia", 111000),
	("Ram", "CEO", "Iran", 300000),
	("Lekhana", "Advertising", "UK", 250000),
	("Thanos", "Marketing", "UIND", 114000),
	("Nick", "Data Engineer", "Ireland", 680000),
	("Wade", "Data Engineer", "IND", 70000)
]

# Column names of dataframe
columns = ["Name", "Job", "Country", "salary"]

# Create the spark dataframe
df = spark.createDataFrame(data=dataframe, schema=columns)

# Printing the dataframe
df.show()

# Sort the dataframe by ascending
# order of 'Name'
df.sort(["Name"],ascending = [True]).show()

#Join 
# importing module
import pyspark

# importing sparksession from pyspark.sql module
from pyspark.sql import SparkSession

# creating sparksession and giving an app name
spark = SparkSession.builder.appName('sparkdf').getOrCreate()

# list of employee data
data = [["1", "sravan", "company 1"],
		["2", "ojaswi", "company 1"],
		["3", "rohith", "company 2"],
		["4", "sridevi", "company 1"],
		["5", "bobby", "company 1"]]

# specify column names
columns = ['ID', 'NAME', 'Company']

# creating a dataframe from the lists of data
dataframe = spark.createDataFrame(data, columns)

# list of employee data
data1 = [["1", "45000", "IT"],
		["2", "145000", "Manager"],
		["6", "45000", "HR"],
		["5", "34000", "Sales"]]

# specify column names
columns = ['ID', 'salary', 'department']

# creating a dataframe from the lists of data
dataframe1 = spark.createDataFrame(data1, columns)


# inner join on two dataframes
dataframe.join(dataframe1,
			dataframe.ID == dataframe1.ID,
			"inner").show()

#Full outer join
# importing module
import pyspark

# importing sparksession from pyspark.sql module
from pyspark.sql import SparkSession

# creating sparksession and giving an app name
spark = SparkSession.builder.appName('sparkdf').getOrCreate()

# list of employee data
data = [["1", "sravan", "company 1"],
		["2", "ojaswi", "company 1"],
		["3", "rohith", "company 2"],
		["4", "sridevi", "company 1"],
		["5", "bobby", "company 1"]]

# specify column names
columns = ['ID', 'NAME', 'Company']

# creating a dataframe from the lists of data
dataframe = spark.createDataFrame(data, columns)

# list of employee data
data1 = [["1", "45000", "IT"],
		["2", "145000", "Manager"],
		["6", "45000", "HR"],
		["5", "34000", "Sales"]]

# specify column names
columns = ['ID', 'salary', 'department']

# creating a dataframe from the lists of data
dataframe1 = spark.createDataFrame(data1, columns)


# full outer join on two dataframes
dataframe.join(dataframe1,
			dataframe.ID == dataframe1.ID,
			"outer").show()

#Full join
# importing module
import pyspark

# importing sparksession from pyspark.sql module
from pyspark.sql import SparkSession

# creating sparksession and giving an app name
spark = SparkSession.builder.appName('sparkdf').getOrCreate()

# list of employee data
data = [["1", "sravan", "company 1"],
		["2", "ojaswi", "company 1"],
		["3", "rohith", "company 2"],
		["4", "sridevi", "company 1"],
		["5", "bobby", "company 1"]]

# specify column names
columns = ['ID', 'NAME', 'Company']

# creating a dataframe from the lists of data
dataframe = spark.createDataFrame(data, columns)

# list of employee data
data1 = [["1", "45000", "IT"],
		["2", "145000", "Manager"],
		["6", "45000", "HR"],
		["5", "34000", "Sales"]]

# specify column names
columns = ['ID', 'salary', 'department']

# creating a dataframe from the lists of data
dataframe1 = spark.createDataFrame(data1, columns)


# full outer join on two dataframes
dataframe.join(dataframe1,
			dataframe.ID == dataframe1.ID,
			"full").show()

#left join
# importing module
import pyspark

# importing sparksession from pyspark.sql module
from pyspark.sql import SparkSession

# creating sparksession and giving an app name
spark = SparkSession.builder.appName('sparkdf').getOrCreate()

# list of employee data
data = [["1", "sravan", "company 1"],
		["2", "ojaswi", "company 1"],
		["3", "rohith", "company 2"],
		["4", "sridevi", "company 1"],
		["5", "bobby", "company 1"]]

# specify column names
columns = ['ID', 'NAME', 'Company']

# creating a dataframe from the lists of data
dataframe = spark.createDataFrame(data, columns)

# list of employee data
data1 = [["1", "45000", "IT"],
		["2", "145000", "Manager"],
		["6", "45000", "HR"],
		["5", "34000", "Sales"]]

# specify column names
columns = ['ID', 'salary', 'department']

# creating a dataframe from the lists of data
dataframe1 = spark.createDataFrame(data1, columns)


# left join on two dataframes
dataframe.join(dataframe1,
			dataframe.ID == dataframe1.ID,
			"left").show()

#left outer join
# importing module
import pyspark

# importing sparksession from pyspark.sql module
from pyspark.sql import SparkSession

# creating sparksession and giving an app name
spark = SparkSession.builder.appName('sparkdf').getOrCreate()

# list of employee data
data = [["1", "sravan", "company 1"],
		["2", "ojaswi", "company 1"],
		["3", "rohith", "company 2"],
		["4", "sridevi", "company 1"],
		["5", "bobby", "company 1"]]

# specify column names
columns = ['ID', 'NAME', 'Company']

# creating a dataframe from the lists of data
dataframe = spark.createDataFrame(data, columns)

# list of employee data
data1 = [["1", "45000", "IT"],
		["2", "145000", "Manager"],
		["6", "45000", "HR"],
		["5", "34000", "Sales"]]

# specify column names
columns = ['ID', 'salary', 'department']

# creating a dataframe from the lists of data
dataframe1 = spark.createDataFrame(data1, columns)

# left join on two dataframes
dataframe.join(dataframe1,
			dataframe.ID == dataframe1.ID,
			"leftouter").show()

#right join
# importing module
import pyspark

# importing sparksession from pyspark.sql module
from pyspark.sql import SparkSession

# creating sparksession and giving an app name
spark = SparkSession.builder.appName('sparkdf').getOrCreate()

# list of employee data
data = [["1", "sravan", "company 1"],
		["2", "ojaswi", "company 1"],
		["3", "rohith", "company 2"],
		["4", "sridevi", "company 1"],
		["5", "bobby", "company 1"]]

# specify column names
columns = ['ID', 'NAME', 'Company']

# creating a dataframe from the lists of data
dataframe = spark.createDataFrame(data, columns)

# list of employee data
data1 = [["1", "45000", "IT"],
		["2", "145000", "Manager"],
		["6", "45000", "HR"],
		["5", "34000", "Sales"]]

# specify column names
columns = ['ID', 'salary', 'department']

# creating a dataframe from the lists of data
dataframe1 = spark.createDataFrame(data1, columns)

# right join on two dataframes
dataframe.join(dataframe1,
			dataframe.ID == dataframe1.ID,
			"right").show()

#rightouter join
# importing module
import pyspark

# importing sparksession from pyspark.sql module
from pyspark.sql import SparkSession

# creating sparksession and giving an app name
spark = SparkSession.builder.appName('sparkdf').getOrCreate()

# list of employee data
data = [["1", "sravan", "company 1"],
		["2", "ojaswi", "company 1"],
		["3", "rohith", "company 2"],
		["4", "sridevi", "company 1"],
		["5", "bobby", "company 1"]]

# specify column names
columns = ['ID', 'NAME', 'Company']

# creating a dataframe from the lists of data
dataframe = spark.createDataFrame(data, columns)

# list of employee data
data1 = [["1", "45000", "IT"],
		["2", "145000", "Manager"],
		["6", "45000", "HR"],
		["5", "34000", "Sales"]]

# specify column names
columns = ['ID', 'salary', 'department']

# creating a dataframe from the lists of data
dataframe1 = spark.createDataFrame(data1, columns)

# right join on two dataframes
dataframe.join(dataframe1,
			dataframe.ID == dataframe1.ID,
			"rightouter").show()

#lefsemi
# importing module
import pyspark

# importing sparksession from pyspark.sql module
from pyspark.sql import SparkSession

# creating sparksession and giving an app name
spark = SparkSession.builder.appName('sparkdf').getOrCreate()

# list of employee data
data = [["1", "sravan", "company 1"],
		["2", "ojaswi", "company 1"],
		["3", "rohith", "company 2"],
		["4", "sridevi", "company 1"],
		["5", "bobby", "company 1"]]

# specify column names
columns = ['ID', 'NAME', 'Company']

# creating a dataframe from the lists of data
dataframe = spark.createDataFrame(data, columns)

# list of employee data
data1 = [["1", "45000", "IT"],
		["2", "145000", "Manager"],
		["6", "45000", "HR"],
		["5", "34000", "Sales"]]

# specify column names
columns = ['ID', 'salary', 'department']

# creating a dataframe from the lists of data
dataframe1 = spark.createDataFrame(data1, columns)

# leftsemi join on two dataframes
dataframe.join(dataframe1,
			dataframe.ID == dataframe1.ID,
			"leftsemi").show()

#left anti
# importing module
import pyspark

# importing sparksession from pyspark.sql module
from pyspark.sql import SparkSession

# creating sparksession and giving an app name
spark = SparkSession.builder.appName('sparkdf').getOrCreate()

# list of employee data
data = [["1", "sravan", "company 1"],
		["2", "ojaswi", "company 1"],
		["3", "rohith", "company 2"],
		["4", "sridevi", "company 1"],
		["5", "bobby", "company 1"]]

# specify column names
columns = ['ID', 'NAME', 'Company']

# creating a dataframe from the lists of data
dataframe = spark.createDataFrame(data, columns)

# list of employee data
data1 = [["1", "45000", "IT"],
		["2", "145000", "Manager"],
		["6", "45000", "HR"],
		["5", "34000", "Sales"]]

# specify column names
columns = ['ID', 'salary', 'department']

# creating a dataframe from the lists of data
dataframe1 = spark.createDataFrame(data1, columns)

# leftanti join on two dataframes
dataframe.join(dataframe1,
			dataframe.ID == dataframe1.ID,
			"leftanti").show()

#Right join
# importing module
import pyspark

# importing sparksession from pyspark.sql module
from pyspark.sql import SparkSession

# creating sparksession and giving an app name
spark = SparkSession.builder.appName('sparkdf').getOrCreate()

# list of employee data
data = [["1", "sravan", "company 1"],
		["2", "ojaswi", "company 1"],
		["3", "rohith", "company 2"],
		["4", "sridevi", "company 1"],
		["5", "bobby", "company 1"]]

# specify column names
columns = ['ID', 'NAME', 'Company']

# creating a dataframe from the lists of data
dataframe = spark.createDataFrame(data, columns)

# list of employee data
data1 = [["1", "45000", "IT"],
		["2", "145000", "Manager"],
		["6", "45000", "HR"],
		["5", "34000", "Sales"]]

# specify column names
columns = ['ID', 'salary', 'department']

# creating a dataframe from the lists of data
dataframe1 = spark.createDataFrame(data1, columns)

# right join on two dataframes
dataframe.join(dataframe1,
			dataframe.ID == dataframe1.ID,
			"right").show()

#right outer
# # importing module
# import pyspark

# # importing sparksession from pyspark.sql module
# from pyspark.sql import SparkSession

# creating sparksession and giving an app name
spark = SparkSession.builder.appName('sparkdf').getOrCreate()

# list of employee data
data = [["1", "sravan", "company 1"],
		["2", "ojaswi", "company 1"],
		["3", "rohith", "company 2"],
		["4", "sridevi", "company 1"],
		["5", "bobby", "company 1"]]

# specify column names
columns = ['ID', 'NAME', 'Company']

# creating a dataframe from the lists of data
dataframe = spark.createDataFrame(data, columns)

# list of employee data
data1 = [["1", "45000", "IT"],
		["2", "145000", "Manager"],
		["6", "45000", "HR"],
		["5", "34000", "Sales"]]

# specify column names
columns = ['ID', 'salary', 'department']

# creating a dataframe from the lists of data
dataframe1 = spark.createDataFrame(data1, columns)

# right join on two dataframes
dataframe.join(dataframe1,
			dataframe.ID == dataframe1.ID,
			"rightouter").show()

#Union
# Python program to illustrate the
# working of union() function

import pyspark
from pyspark.sql import SparkSession

#spark = SparkSession.builder.appName('GeeksforGeeks.com').getOrCreate()
spark = SparkSession.builder.getOrCreate()

# Creating a dataframe
data_frame1 = spark.createDataFrame(
	[("Rahul", 82.98), ("Dhoni", 80.31)],
	["Student Name", "Overall Percentage"]
)

# Creating another dataframe
data_frame2 = spark.createDataFrame(
	[("Sachin", 91.123), ("Virat", 90.51)],
	["Student Name", "Overall Percentage"]
)

# union()
answer = data_frame1.union(data_frame2)

# Print the result of the union()
answer.show()

#union all
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

#creating a dataframe
data_frame1 = spark.createDataFrame(
    [("Devanshi", 99.95), ("Smit", 99.99)],
    ["Student Name", "Overall Percentage"]
)

#Creating another dataframe
data_frame2 = spark.createDataFrame(
    [("Kushal", 99.95), ("Disha", 99.98)],
    ["Student Name", "Overall Percentage"]

)

#unionall
answer = data_frame1.unionAll(data_frame2)

#Print the result of the union()
answer.show()

# for each
# Import the SparkSession class from the pyspark.sql module
from pyspark.sql import SparkSession

# Create a SparkSession with the specified app name
spark = SparkSession.builder.appName(
'Example').getOrCreate()

# Create a DataFrame with three rows,
# containing the names and ages of three people
df = spark.createDataFrame(
	[('Alice', 25), ('Bob', 30),
	('Charlie', 35)], ['name', 'age'])

# Initialize an empty list to store the results
result = []

# Perform an action on each row of the
# DataFrame using the foreach() function
# In this case, the action is to append
# the name and age of each row to the result list
df.foreach(lambda row: result.append((row.name,
									row.age)))

# Collect the rows of the DataFrame into
# a list using the collect() function
result = df.collect()

# Print the resulting list of rows
print(result)

#for each partition
def f(people):
    for person in people:
        print(person.name)
df.foreachPartition(f)

#map
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

data = ["Project",
"Gutenberg’s",
"Alice’s",
"Adventures",
"in",
"Wonderland",
"Project",
"Gutenberg’s",
"Adventures",
"in",
"Wonderland",
"Project",
"Gutenberg’s"]

rdd=spark.sparkContext.parallelize(data)

rdd2=rdd.map(lambda x: (x,1))
for element in rdd2.collect():
    print(element)
    
data = [('James','Smith','M',30),
  ('Anna','Rose','F',41),
  ('Robert','Williams','M',62), 
]

columns = ["firstname","lastname","gender","salary"]
df = spark.createDataFrame(data=data, schema = columns)
df.show()

rdd2=df.rdd.map(lambda x: 
    (x[0]+","+x[1],x[2],x[3]*2)
    )  
df2=rdd2.toDF(["name","gender","new_salary"]   )
df2.show()

#Referring Column Names
rdd2=df.rdd.map(lambda x: 
    (x["firstname"]+","+x["lastname"],x["gender"],x["salary"]*2)
    ) 

#Referring Column Names
rdd2=df.rdd.map(lambda x: 
    (x.firstname+","+x.lastname,x.gender,x.salary*2)
    ) 

def func1(x):
    firstName=x.firstname
    lastName=x.lastname
    name=firstName+","+lastName
    gender=x.gender.lower()
    salary=x.salary*2
    return (name,gender,salary)

rdd2=df.rdd.map(lambda x: func1(x))

#flatmap
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
input_data = ["Python Pool",
        "Damn Solutions",
        "Python pool is best",
        "Basic command in python"]
rdd=spark.sparkContext.parallelize(input_data)
rdd2=rdd.flatMap(lambda x: x.split(" "))
for ele in rdd2.collect():
    print(ele)
    
    
#persist
from pyspark.sql import SparkSession
from pyspark import SparkConf, StorageLevel

app_name = "PySpark - persist() Example"
master = "local[8]"

conf = SparkConf().setAppName(app_name)\
    .setMaster(master)


spark = SparkSession.builder.config(conf=conf) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.createDataFrame([[1, 'A'], [2, 'B']], ['id', 'attr'])
df.persist(storageLevel=StorageLevel.MEMORY_ONLY)
print(df.count())
df.groupBy('id').count().show()
df.explain(mode='extended')

#cache
# import pyspark
# df = spark.createDataFrame([(.2, .3), (.0, .6), (.6, .0), (.2, .1)],
#                   columns=['dogs', 'cats'])
# df
# #with memory
# with df.spark.persist(pyspark.StorageLevel.MEMORY_ONLY) as cached_df:
#     print(cached_df.spark.storage_level)
#     print(cached_df.count())
# #with disk
# with df.spark.persist(pyspark.StorageLevel.DISK_ONLY) as cached_df:
#     print(cached_df.spark.storage_level)
#     print(cached_df.count())
# df = df.spark.persist()
# df.to_pandas().mean(axis=1)

#UDF user defined functions
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType,StringType
from pyspark.sql.functions import udf

spark = SparkSession.builder.appName('UDF PRACTICE').getOrCreate()

cms = ["Name","RawScore"]
data = [("Jack", "79"),
		("Mira", "80"),
		("Carter", "90")]
					
df = spark.createDataFrame(data=data,schema=cms)

df.show()

#array type column using struct type

data = [
 ("James,,Smith",["Java","Scala","C++"],["Spark","Java"],"OH","CA"),
 ("Michael,Rose,",["Spark","Java","C++"],["Spark","Java"],"NY","NJ"),
 ("Robert,,Williams",["CSharp","VB"],["Spark","Python"],"UT","NV")
]

from pyspark.sql.types import StringType, ArrayType,StructType,StructField
schema = StructType([ 
    StructField("name",StringType(),True), 
    StructField("languagesAtSchool",ArrayType(StringType()),True), 
    StructField("languagesAtWork",ArrayType(StringType()),True), 
    StructField("currentState", StringType(), True), 
    StructField("previousState", StringType(), True)
  ])

df = spark.createDataFrame(data=data,schema=schema)
df.printSchema()
df.show()

#array type map 
#create pyspark maptype

from pyspark.sql.types import StringType, MapType
mapCol = MapType(StringType(),StringType(),False)

#Create MapType From StructType

from pyspark.sql.types import StructField, StructType, StringType, MapType
schema = StructType([
    StructField('name', StringType(), True),
    StructField('properties', MapType(StringType(),StringType()),True)
])

#reate a DataFrame by using above StructType schema

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
dataDictionary = [
        ('James',{'hair':'black','eye':'brown'}),
        ('Michael',{'hair':'brown','eye':None}),
        ('Robert',{'hair':'red','eye':'black'}),
        ('Washington',{'hair':'grey','eye':'grey'}),
        ('Jefferson',{'hair':'brown','eye':''})
        ]
df = spark.createDataFrame(data=dataDictionary, schema = schema)
df.printSchema()
df.show(truncate=False)

# nested struct column

data = [
        (("James",None,"Smith"),"OH","M"),
        (("Anna","Rose",""),"NY","F"),
        (("Julia","","Williams"),"OH","F"),
        (("Maria","Anne","Jones"),"NY","M"),
        (("Jen","Mary","Brown"),"NY","M"),
        (("Mike","Mary","Williams"),"OH","M")
        ]

from pyspark.sql.types import StructType,StructField, StringType        
schema = StructType([
    StructField('name', StructType([
         StructField('firstname', StringType(), True),
         StructField('middlename', StringType(), True),
         StructField('lastname', StringType(), True)
         ])),
     StructField('state', StringType(), True),
     StructField('gender', StringType(), True)
     ])
df2 = spark.createDataFrame(data = data, schema = schema)
df2.printSchema()
df2.show(truncate=False) # shows all columns

#select column as in
df2.select("name").show(truncate=False)

#select column name as in
df2.select("name.firstname","name.lastname").show(truncate=False)

#get all the column from struct column
df2.select("name.*").show(truncate=False)

# Explode Array & Map Column	
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('pyspark-by-examples').getOrCreate()

arrayData = [
        ('James',['Java','Scala'],{'hair':'black','eye':'brown'}),
        ('Michael',['Spark','Java',None],{'hair':'brown','eye':None}),
        ('Robert',['CSharp',''],{'hair':'red','eye':''}),
        ('Washington',None,None),
        ('Jefferson',['1','2'],{})]

df = spark.createDataFrame(data=arrayData, schema = ['name','knownLanguages','properties'])
df.printSchema()
df.show()

#array column 
from pyspark.sql.functions import explode
df2 = df.select(df.name,explode(df.knownLanguages))
df2.printSchema()
df2.show()

#map column
from pyspark.sql.functions import explode
df3 = df.select(df.name,explode(df.properties))
df3.printSchema()
df3.show()

#sampling
# Python program to demonstrate random
# sampling in pyspark without replacement

# Import libraries
import pandas as pd
from pyspark.sql import Row
from pyspark.sql import SparkSession

# create the session
spark = SparkSession.builder.getOrCreate()

# Create dataframe by passing list
df = spark.createDataFrame([
	Row(Brand="Redmi", Units=1000000, Performance="Outstanding", Ecofriendly="Yes"),
	Row(Brand="Samsung", Units=900000, Performance="Outstanding", Ecofriendly="Yes"),
	Row(Brand="Nokia", Units=500000, Performance="Excellent", Ecofriendly="Yes"),
	Row(Brand="Motorola",Units=400000, Performance="Average", Ecofriendly="Yes"),
	Row(Brand="Apple", Units=2000000,Performance="Outstanding", Ecofriendly="Yes")
])

# Apply sample() function without replacement
df_mobile_brands = df.sample(False, 0.5, 42)

# Print to the console
df_mobile_brands.show()


# Python program to demonstrate stratified sampling in pyspark

# Import libraries
import pandas as pd
from pyspark.sql import Row
from pyspark.sql import SparkSession

# Create the session
spark = SparkSession.builder.getOrCreate()

# Creating dataframe by passing list
df = spark.createDataFrame([
	Row(Brand="Redmi", Units=1000000, Performance="Outstanding", Ecofriendly="Yes"),
	Row(Brand="Samsung", Units=1000000, Performance="Outstanding", Ecofriendly="Yes"),
	Row(Brand="Nokia", Units=400000, Performance="Excellent", Ecofriendly="Yes"),
	Row(Brand="Motorola",Units=400000, Performance="Average", Ecofriendly="Yes"),
	Row(Brand="OPPO",Units=400000, Performance="Average", Ecofriendly="Yes"),
	Row(Brand="Apple", Units=2000000,Performance="Outstanding", Ecofriendly="Yes")
])

# Applying sampleBy() function
mobile_brands = df.sampleBy("Units", fractions={
1000000: 0.2, 2000000: 0.4, 400000: 0.2}, seed=0)

# Print to the console
mobile_brands.show()

#Partitioning
#hash partitioning
#range partitioning
#using partitionBy

#hash 
# Import required modules
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("hash\
			_partitioning").getOrCreate()

# Create a sample DataFrame
df = spark.createDataFrame([
	(1, "Jen", 25),
	(2, "Joey", 30),
	(3, "Jenscee", 35),
	(4, "Jamika", 40),
	(5, "Jay", 45),
	(6, "Joy", 50)
], ["id", "name", "age"])

# Print the DataFrame
df.show()
# Perform hash partitioning on the
# DataFrame based on the "id" column
df = df.repartition(4, "id")

# Print the elements in each partition
print(df.rdd.glom().collect())

#range
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("range\
			_partitioning").getOrCreate()

# Create a sample DataFrame
df = spark.createDataFrame([
	(1, "Alice", 25),
	(2, "Bob", 30),
	(3, "Charlie", 35),
	(4, "Dave", 40),
	(5, "Eve", 45),
	(6, "Frank", 50)
], ["id", "name", "age"])

# Perform range partitioning on the
# DataFrame based on the "age" column
df = df.repartitionByRange(3, "age")

# Print the elements in each partition
print(df.rdd.glom().collect())

#using partitionBy
# importing module
import pyspark
from pyspark.sql import SparkSession
from pyspark.context import SparkContext

# creating sparksession and giving an app name
spark = SparkSession.builder.appName('sparkdf').getOrCreate()

# create DataFrame
df=spark.read.option(
"header",True).csv("/home/devanshi/Desktop/workspace/Pandas/IMDB-Movie-Data.csv")

# Display schema
df.printSchema()
df.write.option("header", True) \
		.partitionBy("Year") \
		.mode("overwrite") \
		.csv("Year")






