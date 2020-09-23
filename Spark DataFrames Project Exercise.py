# Databricks notebook source
# MAGIC %md # Spark DataFrames Project Exercise 

# COMMAND ----------

# MAGIC %md Let's get some quick practice with your new Spark DataFrame skills, you will be asked some basic questions about some stock market data, in this case Walmart Stock from the years 2012-2017. This exercise will just ask a bunch of questions, unlike the future machine learning exercises, which will be a little looser and be in the form of "Consulting Projects", but more on that later!
# MAGIC 
# MAGIC For now, just answer the questions and complete the tasks below.

# COMMAND ----------

# MAGIC %md #### Use the walmart_stock.csv file to Answer and complete the  tasks below!

# COMMAND ----------

# MAGIC %md #### Start a simple Spark Session

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("walmart_stock").getOrCreate()

# COMMAND ----------

# MAGIC %md #### Load the Walmart Stock CSV File, have Spark infer the data types.

# COMMAND ----------

df = spark.read.csv("dbfs:/FileStore/shared_uploads/nguyenvu2589@gmail.com/walmart_stock.csv", header=True, inferSchema=True)
# df1 = spark.read.format("csv").load("dbfs:/FileStore/shared_uploads/nguyenvu2589@gmail.com/walmart_stock.csv")

# COMMAND ----------

# MAGIC %md #### What are the column names?

# COMMAND ----------

df.columns

# COMMAND ----------

# MAGIC %md #### What does the Schema look like?

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md #### Print out the first 5 columns.

# COMMAND ----------

for i in df.head(5):
  print(i.asDict())

# COMMAND ----------

# MAGIC %md #### Use describe() to learn about the DataFrame.

# COMMAND ----------

df1 = df.describe()

# COMMAND ----------

# MAGIC %md ## Bonus Question!
# MAGIC #### There are too many decimal places for mean and stddev in the describe() dataframe. Format the numbers to just show up to two decimal places. Pay careful attention to the datatypes that .describe() returns, we didn't cover how to do this exact formatting, but we covered something very similar. [Check this link for a hint](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.Column.cast)
# MAGIC 
# MAGIC If you get stuck on this, don't worry, just view the solutions.

# COMMAND ----------

df1.printSchema()
df1.show()

# COMMAND ----------

df2 = df1.select("summary", "Date", df1.Open.cast("Double"), df1.High.cast("Double"), df1.Low.cast("Double"),df1.Close.cast("Double"),df1.Volume.cast("Double"),df1["Adj Close"].cast("Double"),)

# COMMAND ----------

from pyspark.sql.functions import col, format_number, when
# df1.filter((col("summary") == "mean") | (col("summary") == "stddev")).show()
# df1.filter(col("summary").isin(["mean", "stddev"])).with.show()
cols = ["Open", "High", "Low", "Close", "Volume", "Adj Close"]
for col_val in cols:
  df2 = df2.withColumn(col_val, when(col("summary").isin("mean", "stddev"), format_number(col_val, 2).cast("Double")).otherwise(df2[col_val]))

# COMMAND ----------

df2.show()

# COMMAND ----------

# MAGIC %md #### Create a new dataframe with a column called HV Ratio that is the ratio of the High Price versus volume of stock traded for a day.

# COMMAND ----------

df2 = df2.withColumn("HV RAtio", df2.High/df2.Volume)

# COMMAND ----------

# MAGIC %md #### What day had the Peak High in Price?

# COMMAND ----------

# df2.printSchema()
df.orderBy("High").select("Date").head(1)[0][0]

# COMMAND ----------

# MAGIC %md #### What is the mean of the Close column?

# COMMAND ----------

df.groupBy().mean("Close").show()

# COMMAND ----------

# MAGIC %md #### What is the max and min of the Volume column?

# COMMAND ----------

from pyspark.sql.functions import max, min
df.select([max("Volume"), min("Volume")]).show()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md #### How many days was the Close lower than 60 dollars?

# COMMAND ----------

df.filter("Close < 60").count()

# COMMAND ----------

# MAGIC %md #### What percentage of the time was the High greater than 80 dollars ?
# MAGIC #### In other words, (Number of Days High>80)/(Total Days in the dataset)

# COMMAND ----------

(df.filter("High >80").count() / df.count()) * 100

# COMMAND ----------

# MAGIC %md #### What is the Pearson correlation between High and Volume?
# MAGIC #### [Hint](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameStatFunctions.corr)

# COMMAND ----------

from pyspark.sql.functions import corr
df.agg(corr("High", "Volume").alias("corr")).show()
# df.select(corr("High","Volume")).show()

# COMMAND ----------

# MAGIC %md #### What is the max High per year?

# COMMAND ----------

from pyspark.sql.functions import year

df5 = df.withColumn("Year", year(df.Date))
df5.groupBy("Year").max("High").alias("max_high").show()

# COMMAND ----------

# MAGIC %md #### What is the average Close for each Calendar Month?
# MAGIC #### In other words, across all the years, what is the average Close price for Jan,Feb, Mar, etc... Your result will have a value for each of these months. 

# COMMAND ----------

from pyspark.sql.functions import month
df6 = df.withColumn("Month", month(df.Date))
df6.groupBy("Month").mean("Close").alias("avg_close").orderBy('Month').show()

# COMMAND ----------

# MAGIC %md # Great Job!
