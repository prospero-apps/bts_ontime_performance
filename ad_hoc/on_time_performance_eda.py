# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC # Which 3 airlines had the most flights (excluding cancellations)?

# COMMAND ----------

df = spark.read.table("bts_ontime.02_silver.on_time_performance_enriched")

# COMMAND ----------

# DBTITLE 1,Top 3 airlines excluding cancellations
df.filter("cancelled = false").\
    groupBy("airline").\
    count().\
    orderBy("count", ascending=False).\
    limit(3).\
    display()

# COMMAND ----------

# MAGIC %md
# MAGIC # What is the percentage share of each cancellation reason?

# COMMAND ----------

# DBTITLE 1,Cancellation reasons with percentage share
# Group by cancelled + reason so the window can be partitioned by cancelled (avoids the no-partition warning)
df.filter("cancelled = true").\
    groupBy("cancelled", "cancellation_reason").\
    count().\
    withColumn("percentage_share", round(col("count") * 100 / sum("count").over(Window.partitionBy("cancelled")), 2)).\
    drop("cancelled").\
    orderBy("count", ascending=False).\
    display()

# COMMAND ----------

# MAGIC %md
# MAGIC # What are the 3 most common routes?

# COMMAND ----------

# DBTITLE 1,Top 3 routes regardless of direction
# Order each airport pair alphabetically so A->B and B->A count as the same route
df.filter("cancelled = false").\
    withColumn("airport_1", least("origin_airport", "destination_airport")).\
    withColumn("airport_2", greatest("origin_airport", "destination_airport")).\
    groupBy("airport_1", "airport_2").\
    count().\
    orderBy(col("count").desc(), "airport_1", "airport_2").\
    limit(3).\
    display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Show the number of flights per day.

# COMMAND ----------

# DBTITLE 1,Number of flights per day
df.filter("cancelled = false").\
    groupBy("flight_date").\
    agg(count("*").alias("number_of_flights")).\
    orderBy("flight_date").\
    display()