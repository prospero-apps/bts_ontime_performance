# Databricks notebook source
from pyspark.sql.functions import current_timestamp, col, lit, length, substring, when, expr, coalesce, to_date
from pyspark.sql.types import TimestampType, DateType, IntegerType, DoubleType

# COMMAND ----------

# read
df = spark.read.table("bts_ontime.01_bronze.on_time_performance_raw")

# COMMAND ----------

# helpers: 

# build TIMESTAMP_NTZ (local airport time, no time zone) values from HHmm strings
def at_hhmm(date_col, hhmm_col_name):
    # date + HH hours + mm minutes; make_dt_interval rolls '2400' over to 00:00 of the next day
    return to_date(date_col).cast("timestamp_ntz") + expr(
        f"make_dt_interval(0, int(substr({hhmm_col_name}, 1, 2)), int(substr({hhmm_col_name}, 3, 2)), 0)"
    )

def plus_minutes(ts_col, minutes_col_name):
    return ts_col + expr(f"make_dt_interval(0, 0, int(cast({minutes_col_name} as double)), 0)")

def arrival_near(departure_ts, hhmm_col_name):
    # arrival clock time placed on the departure date, then shifted by one day if needed.
    # local arrival - local departure = flight duration + time zone difference, which is within [-6h, +18h] for US flights
    naive = at_hhmm(departure_ts, hhmm_col_name)
    return (
        when(naive < departure_ts - expr("INTERVAL 6 HOURS"), naive + expr("INTERVAL 1 DAY"))
        .when(naive > departure_ts + expr("INTERVAL 18 HOURS"), naive - expr("INTERVAL 1 DAY"))
        .otherwise(naive)
    )

def nearest_to(ref_ts, hhmm_col_name):
    # clock time placed on whichever day (previous / same / next) is within 12 hours of a reference timestamp
    # in the same time zone, e.g. wheels-off vs. actual departure, wheels-on vs. actual arrival
    naive = at_hhmm(ref_ts, hhmm_col_name)
    return (
        when(naive < ref_ts - expr("INTERVAL 12 HOURS"), naive + expr("INTERVAL 1 DAY"))
        .when(naive > ref_ts + expr("INTERVAL 12 HOURS"), naive - expr("INTERVAL 1 DAY"))
        .otherwise(naive)
    )

def to_int(col_name):
    # source values look like '125.00'; ANSI mode rejects a direct string -> int cast, so go via double
    return col(col_name).cast(DoubleType()).cast(IntegerType())

def to_bool(col_name):
    # source flags are '0.00' / '1.00' -> false / true (a direct string -> boolean cast fails in ANSI mode)
    return col(col_name).cast(DoubleType()) == 1.0

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

# filter flight dates
df = df.filter(col("flight_date") >= "2026-07-01").filter(col("flight_date") <= "2026-07-31")

# COMMAND ----------

# write
df.write.mode("overwrite").saveAsTable("bts_ontime.02_silver.on_time_performance_cleansed")