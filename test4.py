# # Data Source API	

#PySpark read and write Parquet file
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

#overwrite an existing dataframe
df.write.mode("overwrite").parquet("/tmp/output/people.parquet")
parDF1=spark.read.parquet("/tmp/output/people.parquet")
parDF1.createOrReplaceTempView("parquetTable")
parDF1.printSchema()
parDF1.show(truncate=False)

#executing SQL queries DataFrame
parkSQL = spark.sql("select * from ParquetTable where salary >= 4000 ")
parkSQL.show(truncate=False)

#creating table on Parquet file
spark.sql("CREATE TEMPORARY VIEW PERSON USING parquet OPTIONS (path \"/tmp/output/people.parquet\")")
spark.sql("SELECT * FROM PERSON").show()

#Create parquet partition file
df.write.partitionBy("gender","salary").mode("overwrite").parquet("/tmp/output/people2.parquet")

#Retrieving from a partitioned Parquet file
parDF2=spark.read.parquet("/tmp/output/people2.parquet/gender=M")
parDF2.show(truncate=False)

#Creating table on partitioned parquet file
spark.sql("CREATE TEMPORARY VIEW PERSON2 USING parquet OPTIONS (path \"/tmp/output/people2.parquet/gender=F\")")
spark.sql("SELECT * FROM PERSON2" ).show()


#AVRO File
# df = spark.read.format("avro").load("examples/src/main/resources/users.avro")
# df.select("name", "favorite_color").write.format("avro").save("namesAndFavColors.avro")

# from pyspark.sql.avro.functions import from_avro, to_avro

# # `from_avro` requires Avro schema in JSON string format.
# # jsonFormatSchema = open("examples/src/main/resources/user.avsc", "r").read()

# df = spark\
#   .readStream\
#   .format("kafka")\
#   .option("kafka.bootstrap.servers", "host1:port1,host2:port2")\
#   .option("subscribe", "topic1")\
#   .load()

# # 1. Decode the Avro data into a struct;
# # 2. Filter by column `favorite_color`;
# # 3. Encode the column `name` in Avro format.
# output = df\
#   .select(from_avro("value", jsonFormatSchema).alias("user"))\
#   .where('user.favorite_color == "red"')\
#   .select(to_avro("user.name").alias("value"))

# query = output\
#   .writeStream\
#   .format("kafka")\
#   .option("kafka.bootstrap.servers", "host1:port1,host2:port2")\
#   .option("topic", "topic2")\
#   .start()