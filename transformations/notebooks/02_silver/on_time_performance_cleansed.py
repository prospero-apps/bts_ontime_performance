# Databricks notebook source
import sys
import os
from pyspark.sql.functions import current_timestamp, col, lit, length, substring, when, expr, coalesce, to_date
from pyspark.sql.functions import max as spark_max
from pyspark.sql.types import TimestampType, DateType, IntegerType, DoubleType

# COMMAND ----------

project_root = os.path.abspath(os.path.join(os.getcwd(), "../.."))

if project_root not in sys.path:
    sys.path.append(project_root)

# COMMAND ----------

from modules.utils.time_utils import at_hhmm, plus_minutes, arrival_near, nearest_to
from modules.utils.type_utils import to_int, to_bool

# COMMAND ----------

# DBTITLE 1,Read only new bronze rows
source_table = "bts_ontime.01_bronze.on_time_performance_raw"
target_table = "bts_ontime.02_silver.on_time_performance_cleansed"

# watermark: latest bronze load already processed into this table (None = nothing processed yet)
watermark = None
if spark.catalog.tableExists(target_table):
    watermark = spark.table(target_table).agg(spark_max("processed_timestamp")).collect()[0][0]
print(f"Latest processed_timestamp in {target_table}: {watermark}")

# read only bronze rows loaded after the watermark
df = spark.read.table(source_table)
if watermark is not None:
    df = df.filter(col("processed_timestamp") > watermark)

# COMMAND ----------

# departures: actual = scheduled + DepDelay (correct date when a delayed flight leaves after midnight);
# fall back to FlightDate + DepTime when DepDelay is missing
scheduled_departure_ts = at_hhmm(col("FlightDate"), "CRSDepTime")
actual_departure_ts = coalesce(plus_minutes(scheduled_departure_ts, "DepDelay"), at_hhmm(col("FlightDate"), "DepTime"))

# arrivals: scheduled = CRSArrTime placed relative to the scheduled departure;
# actual = scheduled arrival + ArrDelay, falling back to ArrTime placed relative to the actual departure (e.g. diverted flights)
scheduled_arrival_ts = arrival_near(scheduled_departure_ts, "CRSArrTime")
actual_arrival_ts = coalesce(plus_minutes(scheduled_arrival_ts, "ArrDelay"), arrival_near(actual_departure_ts, "ArrTime"))

# wheels off (origin local time) is placed relative to the actual departure,
# wheels on (destination local time) relative to the actual arrival
wheels_off_ts = nearest_to(actual_departure_ts, "WheelsOff")
wheels_on_ts = nearest_to(actual_arrival_ts, "WheelsOn")

# COMMAND ----------

# transform
df = df.select(
        col("FlightDate").cast(DateType()).alias("flight_date"),
        when(col("DayOfWeek") == 1, "Monday")
            .when(col("DayOfWeek") == 2, "Tuesday")
            .when(col("DayOfWeek") == 3, "Wednesday")
            .when(col("DayOfWeek") == 4, "Thursday")
            .when(col("DayOfWeek") == 5, "Friday")
            .when(col("DayOfWeek") == 6, "Saturday")
            .when(col("DayOfWeek") == 7, "Sunday")
            .otherwise("Unknown")
            .alias("day_of_week"),
        col("Reporting_Airline").alias("airline_code"),
        col("Tail_Number").alias("tail_number"),
        col("Flight_Number_Reporting_Airline").cast(IntegerType()).alias("flight_number"),
        col("Origin").alias("origin_airport_code"),
        substring(col("OriginCityName"), 1, length(col("OriginCityName")) - 4).alias("origin_city"),
        col("OriginState").alias("origin_state_code"),
        col("Dest").alias("destination_airport_code"),
        substring(col("DestCityName"), 1, length(col("DestCityName")) - 4).alias("destination_city"),
        col("DestState").alias("destination_state_code"),
        scheduled_departure_ts.alias("scheduled_departure_time"),
        actual_departure_ts.alias("actual_departure_time"),
        to_int("DepDelay").alias("departure_delay"),
        to_int("TaxiOut").alias("taxi_out"),
        wheels_off_ts.alias("wheels_off"),
        wheels_on_ts.alias("wheels_on"),
        to_int("TaxiIn").alias("taxi_in"),
        scheduled_arrival_ts.alias("scheduled_arrival_time"),
        actual_arrival_ts.alias("actual_arrival_time"),
        to_int("ArrDelay").alias("arrival_delay"),
        to_bool("Cancelled").alias("cancelled"),
        col("CancellationCode").alias("cancellation_reason_code"),
        to_bool("Diverted").alias("diverted"),
        to_int("AirTime").alias("air_time"),
        to_int("Distance").alias("distance"),
        to_int("CarrierDelay").alias("carrier_delay"),
        to_int("WeatherDelay").alias("weather_delay"),
        to_int("NASDelay").alias("nas_delay"),
        to_int("SecurityDelay").alias("security_delay"),
        to_int("LateAircraftDelay").alias("late_aircraft_delay"),
        col("processed_timestamp")
)

# COMMAND ----------

# write: append only the new rows (skip if there are none)
if df.isEmpty():
    print("No new bronze rows - nothing appended.")
else:
    df.write.mode("append").saveAsTable(target_table)
    print(f"New rows appended to {target_table}")