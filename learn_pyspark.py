import pyspark
import pandas as pd 


# baca_data = pd.read_csv('data.csv')
# print(baca_data)

from pyspark.sql import SparkSession
# gerbang utama
spark= SparkSession.builder.appName('practices').getOrCreate()

# read
df_spark = spark.read.csv('data.csv')

# show data
# df_spark.show()

# read with header
# df_spark=spark.read.option('header', 'true').csv('data.csv').show()

# tipe variable didalamnya
print(type(df_spark))

# read schema
df_spark.printSchema()

