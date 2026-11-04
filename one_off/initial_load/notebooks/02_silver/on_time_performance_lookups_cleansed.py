# Databricks notebook source
from pyspark.sql.functions import current_timestamp, col, lit
from pyspark.sql.types import TimestampType

# COMMAND ----------

# read
df_carriers_lookup = spark.read.table("bts_ontime.01_bronze.carriers_lookup")

# transform
df_carriers_lookup = df_carriers_lookup.select(
    col("Code").alias("code"),
    col("Description").alias("description"),
    current_timestamp().alias("effective_date"),
    lit(None).cast(TimestampType()).alias("end_date")
)

# write
df_carriers_lookup.write.mode("overwrite").saveAsTable("bts_ontime.02_silver.carriers_lookup")

# COMMAND ----------

# read
df_state_lookup = spark.read.table("bts_ontime.01_bronze.state_lookup")

# transform
df_state_lookup = df_state_lookup.select(
    col("Code").alias("code"),
    col("Description").alias("description"),
    current_timestamp().alias("effective_date"),
    lit(None).cast(TimestampType()).alias("end_date")
)

# write
df_state_lookup.write.mode("overwrite").saveAsTable("bts_ontime.02_silver.state_lookup")

# COMMAND ----------

# read
df_airport_lookup = spark.read.table("bts_ontime.01_bronze.airport_lookup")

# transform
df_airport_lookup = df_airport_lookup.select(
    col("Code").alias("code"),
    col("Description").alias("description"),
    current_timestamp().alias("effective_date"),
    lit(None).cast(TimestampType()).alias("end_date")
)

# write
df_airport_lookup.write.mode("overwrite").saveAsTable("bts_ontime.02_silver.airport_lookup")

# COMMAND ----------

# read
df_cancellation_reason_lookup = spark.read.table("bts_ontime.01_bronze.cancellation_reason_lookup")

# transform
df_cancellation_reason_lookup = df_cancellation_reason_lookup.select(
    col("Code").alias("code"),
    col("Description").alias("description"),
    current_timestamp().alias("effective_date"),
    lit(None).cast(TimestampType()).alias("end_date")
)

# write
df_cancellation_reason_lookup.write.mode("overwrite").saveAsTable("bts_ontime.02_silver.cancellation_reason_lookup")