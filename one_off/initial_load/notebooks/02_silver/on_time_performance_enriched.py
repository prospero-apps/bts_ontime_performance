# Databricks notebook source
from pyspark.sql.functions import col

# COMMAND ----------

# read fact and lookup tables
df = spark.read.table("bts_ontime.02_silver.on_time_performance_cleansed")
df_carriers = spark.read.table("bts_ontime.02_silver.carriers_lookup")
df_states = spark.read.table("bts_ontime.02_silver.state_lookup")
df_airports = spark.read.table("bts_ontime.02_silver.airport_lookup")
df_cancellation_reasons = spark.read.table("bts_ontime.02_silver.cancellation_reason_lookup")

# COMMAND ----------

# DBTITLE 1,Join fact table with lookups
# transform
# reduce each lookup to (code, description) and alias it so it can be joined more than once
def lookup(df_lookup, alias):
    return F.broadcast(df_lookup.select("code", "description").alias(alias))

# code column in f -> (lookup alias, new column name); each code column is replaced in place by its description
replacements = {
    "airline_code": ("c", "airline"),
    "origin_state_code": ("os", "origin_state"),
    "destination_state_code": ("ds", "destination_state"),
    "origin_airport_code": ("oa", "origin_airport"),
    "destination_airport_code": ("da", "destination_airport"),
    "cancellation_reason_code": ("cr", "cancellation_reason"),
}

# joins
df_enriched = (
    df.alias("f")
    .join(lookup(df_carriers, "c"), col("f.airline_code") == col("c.code"), "left")
    .join(lookup(df_states, "os"), col("f.origin_state_code") == col("os.code"), "left")
    .join(lookup(df_states, "ds"), col("f.destination_state_code") == col("ds.code"), "left")
    .join(lookup(df_airports, "oa"), col("f.origin_airport_code") == col("oa.code"), "left")
    .join(lookup(df_airports, "da"), col("f.destination_airport_code") == col("da.code"), "left")
    .join(lookup(df_cancellation_reasons, "cr"), col("f.cancellation_reason_code") == col("cr.code"), "left")
    .select([
        col(f"{replacements[c][0]}.description").alias(replacements[c][1]) if c in replacements else col(f"f.{c}")
        for c in df.columns
    ])
)

# COMMAND ----------

# write
df_enriched.write.mode("overwrite").saveAsTable("bts_ontime.02_silver.on_time_performance_enriched")