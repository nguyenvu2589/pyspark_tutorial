# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

spark =SparkSession.builder.appName("ops").getOrCreate()

# COMMAND ----------

df = spark.read.csv("dbfs:/FileStore/shared_uploads/nguyenvu2589@gmail.com/appl_stock.csv", inferSchema=True, header=True)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.head(5)

# COMMAND ----------

# sql filter
df.filter("Close < 500").show()
# python filter
df.filter(df["Close"] > 500).show()

# COMMAND ----------

# sql filter amd select columns
df.filter("Close < 500").select(["Open", "High"]).show()

# COMMAND ----------

#multiple conditions
df.filter((df["Close"] < 200) & (df["Close"] <500)).show()

# COMMAND ----------

# specific val
result = df.filter(df["Low"] == 197.16).collect()

# COMMAND ----------

# convert row to dict
result[0].asDict()

# COMMAND ----------


