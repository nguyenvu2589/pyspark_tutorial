# https://community.cloud.databricks.com/?o=6643098923021310#notebook/1800807208133265/command/1800807208133273

# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

# create session
spark = SparkSession.builder.appName('Basic').getOrCreate()

# COMMAND ----------

# import file from databric
df1 = spark.read.format("csv").load("dbfs:/FileStore/shared_uploads/nguyenvu2589@gmail.com/hello.csv")

# COMMAND ----------

df = spark.read.csv("dbfs:/FileStore/shared_uploads/nguyenvu2589@gmail.com/hello.csv", header=True)

# COMMAND ----------

df.show()
df.describe().show()
df.describe()

# COMMAND ----------

from pyspark.sql.types import StructField, StringType, IntegerType, StructType, NumericType, DateType, FloatType

# COMMAND ----------

data_schema = [StructField("Names", StringType(), True),
               StructField("Age", StringType(), True),
               StructField("Total_Purchase", StringType(), True),
               StructField("Account_Manager", IntegerType(), True),
               StructField("Years", FloatType(), True),
               StructField("Num_Sites", FloatType(), True),
               StructField("Onboard_date", DateType(), True),
               StructField("Location", StringType(), True),
               StructField("Company", StringType(), True),
               StructField("Churn", StringType(), True),
              ]

# COMMAND ----------

# Create schema
final_struc = StructType(fields=data_schema)
df = spark.read.csv("dbfs:/FileStore/shared_uploads/nguyenvu2589@gmail.com/hello.csv", header=True, schema=final_struc)

# COMMAND ----------

df.describe()

# COMMAND ----------


