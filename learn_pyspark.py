import pyspark
import pandas as pd 


# baca_data = pd.read_csv('data.csv')
# print(baca_data)

from pyspark.sql import SparkSession
# gerbang utama
spark= SparkSession.builder.appName('practices').getOrCreate()

df_spark = spark.read.csv('data.csv')
# df_spark.show()
# df_spark=spark.read.option('header', 'true').csv('data.csv').show()
print(type(df_spark))
