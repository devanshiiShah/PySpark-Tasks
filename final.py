#final task
#Import
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.functions import to_date

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
df.show()

#zero format of date,Accurate formate of pyspark 
df = df.withColumn("bdate",to_date(df.birthdate,"M/d/y"))
df.show()

#Add column birth_year for partition use
df = df.withColumn('birth_year', F.year(df.bdate))
df.show()

#Add column birth_month for partition use
df = df.withColumn('birth_month', F.month(df.bdate))
df.show()

#Add column birth_day for partition use
df = df.withColumn('birth_day', F.dayofmonth(df.bdate))
df.show()

#Add Age column from birth-date
df = df.withColumn("age", F.floor(F.datediff(F.current_date(), F.col("bdate"))/365))
df.show()

#Store as csv in local
df.write.format("csv").option('header',True).mode('overwrite').option('sep',',').save('home/desktop/workspace/output.csv')

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
    .partitionBy('gender','birth_year','birth_month','birth_day') \
    .csv('/home/devanshi/partition')

#Business Date
#Find out those users who has salary>22000
df =df.filter(col("salary") > 22000)
df.write.format("csv").option('header',True).mode('overwrite').save("/home/devanshi/BDateSal")

#Find out those users who has age>60
df =df.filter(col("age") > 60)
df.write.format("csv").option('header',True).mode('overwrite').save("/home/devanshi/BDateAge")
  
#Find out who lives in russia
df =df.filter(col("country") == 'Russia')
df.write.format("csv").option('header',True).mode('overwrite').save("/home/devanshi/country")

#Count total employees who did registration in between 2016-02-01 to 2016-02-03.
# df = df.filter(df.Registraion_Date.startswith("2016-02-01"))
# df = df.filter(df.Registraion_Date.endswith("2016-02-03"))
# df.select(df.Registraion_Date, df.Registration_Date.between("2016-02-01", "2016-02-03"))
# df = spark.sql("select Registraion_Date, Registraion_Date from Registraion_Date "  \
#                 " where Registraion_Date between 2016-02-01 and 2016-02-03")
# spark.sql("SELECT Registraion_Date,COUNT(*) FROM Registraion_Date where(col("Registraion_Date").between("2016-02-01","2016-02-03")))
# df = spark.sql("select count(distinct(*)) from Registraion_Date")         
df = df.select("Registraion_Date","id").where(col("Registraion_Date").between("2016-02-01","2016-02-03 "))

df.write.format("csv").option('header',True).mode('overwrite').save("/home/devanshi/emp")

# select * from Registraion_Date where registration is between 2016-02-01 to 2016-02-03