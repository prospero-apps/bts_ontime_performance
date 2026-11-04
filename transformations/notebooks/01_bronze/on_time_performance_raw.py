# Databricks notebook source
import sys
import os
from pyspark.sql.functions import current_timestamp
import re
from pyspark.sql.functions import max as spark_max, to_date

# COMMAND ----------

project_root = os.path.abspath(os.path.join(os.getcwd(), "../.."))

if project_root not in sys.path:
    sys.path.append(project_root)

# COMMAND ----------

from modules.transformations.metadata import add_processed_timestamp

# COMMAND ----------

# DBTITLE 1,Load only new monthly files from landing zone
landing_path = "/Volumes/bts_ontime/00_landing/data_sources/on_time_performance"
bronze_table = "bts_ontime.01_bronze.on_time_performance_raw"

# 1) Latest year/month already loaded in the bronze table (None = nothing loaded yet)
#    FlightDate is stored as a string (yyyy-MM-dd) in bronze, so convert it to a date first
latest_month = None
if spark.catalog.tableExists(bronze_table):
    max_date = spark.table(bronze_table).agg(spark_max(to_date("FlightDate"))).collect()[0][0]
    if max_date is not None:
        latest_month = (max_date.year, max_date.month)
print(f"Latest month in {bronze_table}: {latest_month}")

# 2) Landing folders named YYYY-MM that are later than the latest loaded month
new_paths = []
for f in dbutils.fs.ls(landing_path):
    m = re.fullmatch(r"(\d{4})-(\d{2})", f.name.rstrip("/"))
    if m and (latest_month is None or (int(m.group(1)), int(m.group(2))) > latest_month):
        new_paths.append(f.path)
new_paths.sort()
print(f"New folders to load: {new_paths if new_paths else 'none'}")

# 3) Read and process only the new folders (df = None if there is nothing new)
df = None
if new_paths:
  df = (spark.read.format("csv")
    .option("header", "true")
    .load(new_paths)
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

  df = add_processed_timestamp(df)

# COMMAND ----------

# Write to the bronze table (only if new files were found)
if df is not None:
    df.write.mode("append").saveAsTable(bronze_table)
    print(f"New rows appended to {bronze_table}")
else:
    print("No new files - nothing appended.")