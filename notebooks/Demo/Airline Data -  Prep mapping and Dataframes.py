# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Step 1: Set the data location and type
# MAGIC 
# MAGIC There are two ways to access Azure Blob storage: account keys and shared access signatures (SAS).
# MAGIC 
# MAGIC To get started, we need to set the location and type of the file.

# COMMAND ----------

storage_account_name = "wtndatalakegen2"
storage_account_access_key = "Zq55yROlbXOonuLsxACg+61Dik5nx9Sl3ZVFeS50LXfeDC6FceT1fLdQIDUEzwYXxSz9p4HlhTe4NU8q/Pav8g=="

# COMMAND ----------

file_location = "abfss://airlines@wtndatalakegen2.dfs.core.windows.net/"
file_type = "csv"

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".dfs.core.windows.net",
  storage_account_access_key)

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC dbutils.fs.rm('abfss://airlines@wtndatalakegen2.dfs.core.windows.net/processed/performance-all.parquet', True)
# MAGIC dbutils.fs.rm("abfss://airlines@wtndatalakegen2.dfs.core.windows.net/processed/airlines.parquet/", True)
# MAGIC dbutils.fs.rm("abfss://airlines@wtndatalakegen2.dfs.core.windows.net/processed/airports.parquet/", True)
# MAGIC dbutils.fs.rm("abfss://airlines@wtndatalakegen2.dfs.core.windows.net/processed/delayed-all.parquet/", True)
# MAGIC dbutils.fs.rm("abfss://airlines@wtndatalakegen2.dfs.core.windows.net/processed/monthlydelayedflights-all.parquet/", True)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 2: Read the data
# MAGIC 
# MAGIC Now that we have specified our file metadata, we can create a DataFrame. Notice that we use an *option* to specify that we want to infer the schema from the file. We can also explicitly set this to a particular schema if we have one already.
# MAGIC 
# MAGIC First, let's create a DataFrame in Python.

# COMMAND ----------

from pyspark.sql.functions import col
airlinesDF = spark.read.format(file_type).option("inferSchema", "true").option("header","true").load(file_location + "Airlines/airline-information.csv" )
airportsDF=spark.read.format(file_type).option("inferSchema", "true").option("header","true").load(file_location + "Airports/airport-information.csv" )
#performance2005DF=spark.read.format(file_type).option("inferSchema", "true").option("header","true").load(file_location + "Performance/2005/*.csv" )
performanceDF=spark.read.format(file_type).option("inferSchema", "true").option("header","true").load(file_location + "Performance/*/*.csv" )
## add in performance pYear to be used as partition key
performanceDF=  performanceDF.withColumn("pYear", col("Year") )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Action
# MAGIC Do some data cleansiing here with the performanceDf dataset which holds all the data. 
# MAGIC Create new dataframes with the processed data
# MAGIC - SQL 
# MAGIC - PySpark

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 3: Query the data
# MAGIC 
# MAGIC Now that we have created our DataFrame, we can query it. For instance, you can identify particular columns to select and display.

# COMMAND ----------

display(airportsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 4: (Optional) Create a view or table
# MAGIC 
# MAGIC If you want to query this data as a table, you can simply register it as a *view* or a table.

# COMMAND ----------

airlinesDF.createOrReplaceTempView("airlines")
airportsDF.createOrReplaceTempView("airports")
performance2005DF.createOrReplaceTempView("performance2005")

# COMMAND ----------

display(airlinesDF)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We can query this view using Spark SQL. For instance, we can perform a simple aggregation. Notice how we can use `%sql` to query the view from SQL.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT UniqueCarrier, FlightNum, count(*) as DelayedFlights FROM performance2005 Where DepDelayMinutes>0
# MAGIC GROUP BY UniqueCarrier, FlightNum

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Since this table is registered as a temp view, it will be available only to this notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.

# COMMAND ----------

#performance2005DF.write.parquet("abfss://airlines@wtndatalakegen2.dfs.core.windows.net/processed/performance-2005.parquet")
performanceDF.write.partitionBy("pYear").parquet("abfss://airlines@wtndatalakegen2.dfs.core.windows.net/processed/performance-all.parquet")
# useful reference
# https://docs.databricks.com/spark/latest/dataframes-datasets/introduction-to-dataframes-python.html#write-the-unioned-dataframe-to-a-parquet-file



# COMMAND ----------

# MAGIC %python
# MAGIC # persist the other data sets as dimensions, for using as parquet
# MAGIC airlinesDF.write.parquet("abfss://airlines@wtndatalakegen2.dfs.core.windows.net/processed/airlines.parquet")
# MAGIC airportsDF.write.parquet("abfss://airlines@wtndatalakegen2.dfs.core.windows.net/processed/airports.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC This table will persist across cluster restarts and allow various users across different notebooks to query this data.

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Exploration
# MAGIC Table data
# MAGIC   - airlines
# MAGIC   - airport information
# MAGIC   - on time performance
# MAGIC   
# MAGIC   
# MAGIC   
# MAGIC Datasets
# MAGIC - Delayed flights: where delayminutes >0
# MAGIC - monthly statistic delayed: count delayed flights, average delay by year, month, airline
# MAGIC - airports monthly statistic delayed: count delayed flights, average delay by year, month, airport
# MAGIC - ranking of on time : count delayed flights/ count total flights by year, month, airline, flight no
# MAGIC - ranking of on time by airport: count delayed flights / count total flights by year, month, airport
# MAGIC 
# MAGIC ### Persist as parquet files in processed area, partitioned by year

# COMMAND ----------

# MAGIC %python
# MAGIC # consolidated delays
# MAGIC 
# MAGIC from pyspark.sql.functions import year, month, col
# MAGIC delayedflightsDF =  spark.read.parquet("abfss://airlines@wtndatalakegen2.dfs.core.windows.net/processed/performance-all.parquet")
# MAGIC delayedflightsDF=delayedflightsDF.filter("DepDelayMinutes>0")
# MAGIC delayedflightsDF=delayedflightsDF.withcolumn("pYear", col("Year"))
# MAGIC # persist delayed flights dataset, as is going to be baseline for other analysis
# MAGIC delayedflightsDF.write.partitionBy("pYear").parquet("abfss://airlines@wtndatalakegen2.dfs.core.windows.net/processed/delayed-all.parquet")

# COMMAND ----------

display(delayedflightsDF.limit(10))

# COMMAND ----------

# MAGIC %python
# MAGIC # monthly statistic delayed: count delayed flights, average delay by year, month, airline
# MAGIC from pyspark.sql.functions import year, month, col, count, avg
# MAGIC 
# MAGIC monthlydelayedflightsDF=delayedflightsDF.select("year", "month","UniqueCarrier", "DepDelayMinutes").groupBy("year", "month", "UniqueCarrier").agg(count("UniqueCarrier").alias("TotalDelayed"), avg("DepDelayMinutes").alias("AverageDepartureDelay"))
# MAGIC 
# MAGIC monthlydelayedflightsDF.write.parquet("abfss://airlines@wtndatalakegen2.dfs.core.windows.net/processed/monthlydelayedflights-all.parquet")

# COMMAND ----------

