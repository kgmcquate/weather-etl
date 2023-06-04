
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.sql("SELECT 'hello_world'")

print(df.collect())

