# Databricks notebook source
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

df_carriers_lookup = spark.read.format("csv").option("header", "true").load("/Volumes/bts_ontime/00_landing/data_sources/lookups/L_UNIQUE_CARRIERS.csv")
df_carriers_lookup.write.mode("overwrite").saveAsTable("bts_ontime.01_bronze.carriers_lookup")

# COMMAND ----------

df_state_lookup = spark.read.format("csv").option("header", "true").load("/Volumes/bts_ontime/00_landing/data_sources/lookups/L_STATE_ABR_AVIATION.csv")
df_state_lookup.write.mode("overwrite").saveAsTable("bts_ontime.01_bronze.state_lookup")

# COMMAND ----------

df_airport_lookup = spark.read.format("csv").option("header", "true").load("/Volumes/bts_ontime/00_landing/data_sources/lookups/L_AIRPORT.csv")
df_airport_lookup.write.mode("overwrite").saveAsTable("bts_ontime.01_bronze.airport_lookup")

# COMMAND ----------

df_cancellation_reason_lookup = spark.read.format("csv").option("header", "true").load("/Volumes/bts_ontime/00_landing/data_sources/lookups/L_CANCELLATION.csv")
df_cancellation_reason_lookup.write.mode("overwrite").saveAsTable("bts_ontime.01_bronze.cancellation_reason_lookup")