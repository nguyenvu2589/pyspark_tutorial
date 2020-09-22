# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.appName("agg").getOrCreate()

# COMMAND ----------

df = spark.read.csv("dbfs:/FileStore/shared_uploads/nguyenvu2589@gmail.com/sales_info.csv", inferSchema=True, header=True)

# COMMAND ----------

df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.groupBy("company").mean().show()
df.groupBy("company").sum().show()
df.groupBy("company").count().show()

# COMMAND ----------

df.agg({"Sales": "sum"}).show()
df.agg({"Sales": "max"}).show()

# COMMAND ----------

group_data = df.groupby("Company")


# COMMAND ----------

from pyspark.sql.functions import countDistinct, avg, stddev
from pyspark.sql.functions import format_number

# COMMAND ----------

df.select(avg("Sales").alias("average_sale")).show()

# COMMAND ----------

sale_std = df.select(stddev("Sales").alias("std"))

# COMMAND ----------

sale_std.select(format_number("std", 2)).show()

# COMMAND ----------

df.show()

# COMMAND ----------

df.orderBy(df["Company"].desc(),df["Sales"].desc()).show()

# COMMAND ----------


