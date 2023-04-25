# # #pyspark
# #SparkSession:Get Existing Session,Spark Config,Spark session with Hive Enable
# #SparkContext:Spark RDD, Read text file,Read CSV,Create RDD,Create Empty RDD,Transformations, Actions,Pair Functions,Repartition and Coalesce,Shuffle Partitions,Cache vs Persist,Persistance Storage,Broadcase Variables,Convert RDD to Dataframe

# # # Create SparkSession from builder
# # import pyspark
# # from pyspark.sql import SparkSession
# # spark = SparkSession.builder.master("local[1]") \
# #                      .appName('SparkByExamples.com') \
# #                      .getOrCreate()



# # # Create new SparkSession
# # spark2 = SparkSession.newSession
# # print(spark2)


# # # Get Existing SparkSession
# # spark3 = SparkSession.builder.getOrCreate
# # print(spark3)


# # # Usage of config()
# # spark = #pyspark
# # # Create SparkSession from builder
# # import pyspark
# # from pyspark.sql import SparkSession
# # spark = SparkSession.builder.master("local[1]") \
# #                      .appName('SparkByExamples.com') \
# #                      .getOrCreate()



# # # Create new SparkSession
# # spark2 = SparkSession.newSession
# # print(spark2)


# # Get Existing SparkSession
# # spark3 = SparkSession.builder.getOrCreate
# # print(spark3)
# # SparkSession.builder \
# #       .master("local[1]") \
# #       .appName("SparkByExamples.com") \
# #       .config("spark.some.config.option", "config-value") \
# #       .getOrCreate()


# # # Enabling Hive to use in Spark
# # spark = SparkSession.builder \
# #       .master("local[1]") \
# #       .appName("SparkByExamples.com") \
# #       .config("spark.sql.warehouse.dir", "<path>/spark-warehouse") \
# #       .enableHiveSupport() \
# #       .getOrCreate()


# #Create SparkSession from builder
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]") \
                    .appName('SparkByExamples.com') \
                    .getOrCreate()
print(spark.sparkContext)
print("Spark App Name : "+ spark.sparkContext.appName)

# Outputs
#<SparkContext master=local[1] appName=SparkByExamples.com>
#Spark App Name : SparkByExamples.com


#SparkContext stop() method
spark.sparkContext.stop()

# conf = SparkConf().setAppName("app1").setMaster("local")
# sc = SparkContext(conf=conf)

# Create SparkContext
from pyspark import SparkContext
sc = SparkContext("local", "Spark_Example_App")
print(sc.appName)


# Create Spark Context
from pyspark import SparkConf, SparkContext
conf = SparkConf()
conf.setMaster("local").setAppName("Spark Example App")
sc = SparkContext.getOrCreate(conf)
print(sc.appName)


# Create RDD
# rdd = spark.sparkContext.range(1, 5)
# print(rdd.collect())

# Output
#[1, 2, 3, 4]


#Create RDD from parallelize    
# data = [1,2,3,4,5,6,7,8,9,10,11,12]
# rdd=spark.sparkContext.parallelize(data)

# # Creates empty RDD with no partition    
# rdd = spark.sparkContext.emptyRDD 
# # rddString = spark.sparkContext.emptyRDD[String]


# #Create RDD from external Data source
# rdd2 = spark.sparkContext.textFile("/path/textFile.txt")

# #create empty RDD with no partition
# rdd = spark.sparkContext.emptyRDD

# #RDD transformation
# rdd = spark.sparkContext.textFile("/tmp/test.txt")

# #flatmap
# rdd2 = rdd.flatMap(lambda x: x.split(" "))

# #map
# rdd3 = rdd2.map(lambda x: (x,1))

# #reducebykey
# rdd4 = rdd3.reducebykey(lambda a,b: a+b)

# rdd5 = rdd4.map(lambda x: (x[1],x[0])).sortByKey()

# #Print rdd5 result to console
# print(rdd5.collect())

# #filter
# rdd6 = rdd5.filter(lambda x : 'a' in x[1])
# for element in rdd6.collect():
#     print(element)

# #RDD Action

# # Action - count
# print("Count : "+str(rdd6.count()))

# # Action - first
# firstRec = rdd6.first()
# print("First Record : "+str(firstRec[0]) + ","+ firstRec[1])

# # Action - max
# datMax = rdd6.max()
# print("Max Record : "+str(datMax[0]) + ","+ datMax[1])

# # Action - reduce
# totalWordCount = rdd6.reduce(lambda a,b: (a[0]+b[0],a[1]))
# print("dataReduce Record : "+str(totalWordCount[0]))

# # Action - take
# data3 = rdd6.take(3)
# for f in data3:
#     print("data3 Key:"+ str(f[0]) +", Value:"+f[1])

# # Action - collect
# data = rdd6.collect()
# for f in data:
#     print("Key:"+ str(f[0]) +", Value:"+f[1])

# rdd6.saveAsTextFile("/tmp/wordCount")

#pair function
#import the pyspark module
import pyspark

#import SparkSession for creating a session
from pyspark.sql import SparkSession

# import RDD from pyspark.rdd
from pyspark.rdd import RDD

#create an app named linuxhint
spark_app = SparkSession.builder.appName('linuxhint').getOrCreate()

# create 6 - subject and rating pairs
subjects_rating =spark_app.sparkContext.parallelize([('python',4),('javascript',2),('linux',5),('C#',4),('javascript',4),('python',3)])

#apply reduceByKey() transformation on the above subjects_rating to reduce Keys by adding values with similar keys
print(subjects_rating.reduceByKey(lambda element1, element2: element1 + element2).collect())

#apply reduceByKey() transformation on the above subjects_rating to reduce Keys by subtracting values from similar keys
print(subjects_rating.reduceByKey(lambda element1, element2: element1 - element2).collect())

#apply reduceByKey() transformation on the above subjects_rating to reduce Keys by multiplying values with similar keys
print(subjects_rating.reduceByKey(lambda element1, element2: element1 * element2).collect())

#apply reduceByKey() transformation on the above subjects_rating to reduce Keys by dividing values with similar keys
print(subjects_rating.reduceByKey(lambda element1, element2: element1 / element2).collect())




