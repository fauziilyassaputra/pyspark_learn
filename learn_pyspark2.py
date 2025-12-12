import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Dataframe').getOrCreate()

# read dataset --  inferscehema True supaya bisa membaca data integer didalam data 
dfspark = spark.read.option('header','true').csv('data_3.csv', inferSchema=True) 

# check the schema
dfspark.printSchema()

#
dfspark = spark.read.csv('data_3.csv', header=True, inferSchema=True)
dfspark.show()

# read column
print(dfspark.columns)

# read head
print(dfspark.head(3))

# select  
dfspark.select('Name').show()

# read tipe data
print('membaca tipe data di column name :')
print(type(dfspark.select('Name')))
