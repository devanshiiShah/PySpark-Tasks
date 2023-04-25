# join two dataframe
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

# create a view for dataframe named student
dataframe.createOrReplaceTempView("student")

# create a view for dataframe1 named department
dataframe1.createOrReplaceTempView("department")

#use sql expression to select ID column
spark.sql(
	"select * from student, department\
	where student.ID == department.ID").show()


