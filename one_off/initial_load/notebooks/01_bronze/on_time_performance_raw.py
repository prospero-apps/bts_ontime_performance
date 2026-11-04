# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
import sys
import os
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

project_root = os.path.abspath(os.path.join(os.getcwd(), "../.."))

if project_root not in sys.path:
    sys.path.append(project_root)

# COMMAND ----------

from modules.transformations.metadata import add_processed_timestamp

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
)

# COMMAND ----------

df = add_processed_timestamp(df)

# COMMAND ----------

# Write to the bronze table
df.write.mode("overwrite").saveAsTable("bts_ontime.01_bronze.on_time_performance_raw")