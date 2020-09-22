# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import mean

# COMMAND ----------

spark = SparkSession.builder.appName("missing").getOrCreate()

# COMMAND ----------

df = spark.read.csv("dbfs:/FileStore/shared_uploads/nguyenvu2589@gmail.com/ContainsNull.csv", header=True, inferSchema=True)

# COMMAND ----------

df.show()

# COMMAND ----------

# must have at least 2 non null val
df.na.drop(thresh=2).show()

# COMMAND ----------

df.na.drop(how="all").show()

# COMMAND ----------

# drop if Sales is null
df.na.drop(subset=["Sales"]).show()

# COMMAND ----------

# fill "No Name" value for null Name
df.na.fill("No Name", subset=["Name"]).show()

# COMMAND ----------

# fill mean val into null sales
mean_val = df.select(mean(df["Sales"])).collect()
df.na.fill(mean_val[0][0], "Sales").show()

# COMMAND ----------



# COMMAND ----------


