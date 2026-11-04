# Databricks notebook source
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# Read from the landing zone and process
df = (spark.read.format("csv")
    .option("header", "true")
    .load("/Volumes/bts_ontime/00_landing/data_sources/on_time_performance/*")
    .select(
        "FlightDate",
        "DayOfWeek",
        "Reporting_Airline",
        "Tail_Number",
        "Flight_Number_Reporting_Airline",
        "Origin",
        "OriginCityName",
        "OriginState",
        "Dest",
        "DestCityName",
        "DestState",
        "CRSDepTime",
        "DepTime",
        "DepDelay",
        "TaxiOut",
        "WheelsOff",
        "WheelsOn",
        "TaxiIn",
        "CRSArrTime",
        "ArrTime",
        "ArrDelay",
        "Cancelled",
        "CancellationCode",
        "Diverted",
        "AirTime",
        "Distance",
        "CarrierDelay",
        "WeatherDelay",
        "NASDelay",
        "SecurityDelay",
        "LateAircraftDelay"
    )
    .withColumn("processed_timestamp", current_timestamp())
)

# COMMAND ----------

# Write to the bronze table
df.write.mode("overwrite").saveAsTable("bts_ontime.01_bronze.on_time_performance_raw")