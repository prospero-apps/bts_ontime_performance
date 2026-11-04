# Databricks notebook source
# DBTITLE 1,Imports
from pyspark.sql.functions import max, min, count, avg, round, when, col, coalesce, sum, lit

# COMMAND ----------

# DBTITLE 1,Read only flight dates not yet summarized
source_table = "bts_ontime.02_silver.on_time_performance_enriched"
target_table = "bts_ontime.03_gold.daily_on_time_performance_summary"

# watermark: latest flight_date already summarized in the gold table (None = nothing summarized yet)
# the gold table is aggregated per flight_date, so flight_date is the natural watermark here
watermark = None
if spark.catalog.tableExists(target_table):
    watermark = spark.table(target_table).agg(max("flight_date")).collect()[0][0]
print(f"Latest flight_date in {target_table}: {watermark}")

# read only rows for flight dates after the watermark
df = spark.read.table(source_table)
if watermark is not None:
    df = df.filter(col("flight_date") > lit(watermark))

# COMMAND ----------

# DBTITLE 1,Daily aggregation
# transform
df = df.\
    groupBy("flight_date").\
    agg(
        count("*").alias("total_flights"),
        count(when(col("cancelled"), True)).alias("total_cancellations"),
        count(when(col("diverted"), True)).alias("total_diversions"),
        max("departure_delay").alias("max_departure_delay"),
        min("departure_delay").alias("min_departure_delay"),
        round(avg("departure_delay"), 1).alias("avg_departure_delay"),
        max("arrival_delay").alias("max_arrival_delay"),        
        min("arrival_delay").alias("min_arrival_delay"),
        round(avg("arrival_delay"), 1).alias("avg_arrival_delay"),
        max("air_time").alias("max_air_time"),
        min("air_time").alias("min_air_time"),
        round(avg("air_time"), 1).alias("avg_air_time"),
        sum("air_time").alias("total_air_time"),
        round(avg("distance"), 1).alias("avg_distance"),
        sum("distance").alias("total_distance"),
        coalesce(sum("carrier_delay"), lit(0)).alias("total_carrier_delay"),
        coalesce(sum("weather_delay"), lit(0)).alias("total_weather_delay"),
        coalesce(sum("nas_delay"), lit(0)).alias("total_nas_delay"),
        coalesce(sum("security_delay"), lit(0)).alias("total_security_delay"),
        coalesce(sum("late_aircraft_delay"), lit(0)).alias("total_late_aircraft_delay")
    )

# COMMAND ----------

# DBTITLE 1,Append new daily summaries
# write: append only the new days (skip if there are none)
if df.isEmpty():
    print("No new flight dates - nothing appended.")
else:
    df.write.mode("append").saveAsTable(target_table)
    print(f"New daily rows appended to {target_table}")