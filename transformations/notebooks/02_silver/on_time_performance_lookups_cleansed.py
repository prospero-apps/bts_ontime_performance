# Databricks notebook source
from pyspark.sql.functions import current_timestamp, col, lit, when
from pyspark.sql.types import TimestampType
from datetime import datetime
from delta.tables import DeltaTable

# COMMAND ----------

# Fixed point in time used to close any changed active records
end_timestamp = datetime.now()

# COMMAND ----------

# MAGIC %md
# MAGIC # Carriers Lookup

# COMMAND ----------

# read
df_carriers_lookup = spark.read.table("bts_ontime.01_bronze.carriers_lookup")
dt_carriers = DeltaTable.forName(spark, "bts_ontime.02_silver.carriers_lookup")

# COMMAND ----------

# transform
df_carriers_lookup = df_carriers_lookup.select(
    col("Code").alias("code"),
    col("Description").alias("description"),
    current_timestamp().alias("effective_date"),
    lit(None).cast(TimestampType()).alias("end_date")
)

# COMMAND ----------

# close changed active rows
dt_carriers.alias("tc").\
    merge(
        source = df_carriers_lookup.alias("sc"),
        condition = "tc.code = sc.code AND tc.end_date IS NULL AND tc.description != sc.description"
    ).whenMatchedUpdate(
        set = {
            "tc.end_date": lit(end_timestamp).cast(TimestampType())
        }
    ).execute()       

# COMMAND ----------

# insert new current versions
# new rows are inserted for keys we just closed and new keys

# list of keys we just closed
closed_codes = [row.code for row in dt_carriers.toDF().\
    filter(f"end_date = '{end_timestamp}'").\
    select("code").\
    collect()]

if len(closed_codes) == 0:
    print("No updated records to insert")
else:
    dt_carriers.alias("tc").\
        merge(
            source = df_carriers_lookup.alias("sc"),
            condition = f"tc.code not in ({', '.join(map(str, closed_codes))})"
        ).whenNotMatchedInsert(
            values = {
                "code": "sc.code",
                "description": "sc.description",
                "effective_date": current_timestamp(),
                "end_date": lit(None).cast(TimestampType())
            }
        ).execute()

# COMMAND ----------

# insert new keys
dt_carriers.alias("tc").\
    merge(
        source = df_carriers_lookup.alias("sc"),
        condition = "tc.code = sc.code"
    ).whenNotMatchedInsert(
        values = {
            "code": "sc.code",
            "description": "sc.description",
            "effective_date": current_timestamp(),
            "end_date": lit(None).cast(TimestampType())
        }
    ).execute()

# COMMAND ----------

# MAGIC %md
# MAGIC # State Lookup

# COMMAND ----------

# read
df_state_lookup = spark.read.table("bts_ontime.01_bronze.state_lookup")
dt_state = DeltaTable.forName(spark, "bts_ontime.02_silver.state_lookup")

# COMMAND ----------

# transform
df_state_lookup = df_state_lookup.select(
    col("Code").alias("code"),
    col("Description").alias("description"),
    current_timestamp().alias("effective_date"),
    lit(None).cast(TimestampType()).alias("end_date")
)

# COMMAND ----------

# close changed active rows
dt_state.alias("ts").\
    merge(
        source = df_state_lookup.alias("ss"),
        condition = "ts.code = ss.code AND ts.end_date IS NULL AND ts.description != ss.description"
    ).whenMatchedUpdate(
        set = {
            "ts.end_date": lit(end_timestamp).cast(TimestampType())
        }
    ).execute()       

# COMMAND ----------

# insert new current versions
# new rows are inserted for keys we just closed and new keys

# list of keys we just closed
closed_codes = [row.code for row in dt_state.toDF().\
    filter(f"end_date = '{end_timestamp}'").\
    select("code").\
    collect()]

if len(closed_codes) == 0:
    print("No updated records to insert")
else:
    dt_state.alias("ts").\
        merge(
            source = df_state_lookup.alias("ss"),
            condition = f"ts.code not in ({', '.join(map(str, closed_codes))})"
        ).whenNotMatchedInsert(
            values = {
                "code": "ss.code",
                "description": "ss.description",
                "effective_date": current_timestamp(),
                "end_date": lit(None).cast(TimestampType())
            }
        ).execute()

# COMMAND ----------

# insert new keys
dt_state.alias("ts").\
    merge(
        source = df_state_lookup.alias("ss"),
        condition = "ts.code = ss.code"
    ).whenNotMatchedInsert(
        values = {
            "code": "ss.code",
            "description": "ss.description",
            "effective_date": current_timestamp(),
            "end_date": lit(None).cast(TimestampType())
        }
    ).execute()


# COMMAND ----------

# MAGIC %md
# MAGIC # Airport Lookup

# COMMAND ----------

# read
df_airport_lookup = spark.read.table("bts_ontime.01_bronze.airport_lookup")
dt_airport = DeltaTable.forName(spark, "bts_ontime.02_silver.airport_lookup")

# COMMAND ----------

# transform
df_airport_lookup = df_airport_lookup.select(
    col("Code").alias("code"),
    col("Description").alias("description"),
    current_timestamp().alias("effective_date"),
    lit(None).cast(TimestampType()).alias("end_date")
)

# COMMAND ----------

# close changed active rows
dt_airport.alias("ta").\
    merge(
        source = df_airport_lookup.alias("sa"),
        condition = "ta.code = sa.code AND ta.end_date IS NULL AND ta.description != sa.description"
    ).whenMatchedUpdate(
        set = {
            "ta.end_date": lit(end_timestamp).cast(TimestampType())
        }
    ).execute()       

# COMMAND ----------

# insert new current versions
# new rows are inserted for keys we just closed and new keys

# list of keys we just closed
closed_codes = [row.code for row in dt_airport.toDF().\
    filter(f"end_date = '{end_timestamp}'").\
    select("code").\
    collect()]

if len(closed_codes) == 0:
    print("No updated records to insert")
else:
    dt_airport.alias("ta").\
        merge(
            source = df_airport_lookup.alias("sa"),
            condition = f"ta.code not in ({', '.join(map(str, closed_codes))})"
        ).whenNotMatchedInsert(
            values = {
                "code": "sa.code",
                "dscription": "sa.description",
                "effective_date": current_timestamp(),
                "end_date": lit(None).cast(TimestampType())
            }
        ).execute()

# COMMAND ----------

# insert new keys
dt_airport.alias("ta").\
    merge(
        source = df_airport_lookup.alias("sa"),
        condition = "ta.code = sa.code"
    ).whenNotMatchedInsert(
        values = {
            "code": "sa.code",
            "description": "sa.description",
            "effective_date": current_timestamp(),
            "end_date": lit(None).cast(TimestampType())
        }
    ).execute()

# COMMAND ----------

# MAGIC %md
# MAGIC # Cancellation Reason Lookup

# COMMAND ----------

# read
df_cancellation_reason_lookup = spark.read.table("bts_ontime.01_bronze.cancellation_reason_lookup")
dt_cancellation_reason = DeltaTable.forName(spark, "bts_ontime.02_silver.cancellation_reason_lookup")

# COMMAND ----------

# transform
df_cancellation_reason_lookup = df_cancellation_reason_lookup.select(
    col("Code").alias("code"),
    col("Description").alias("description"),
    current_timestamp().alias("effective_date"),
    lit(None).cast(TimestampType()).alias("end_date")
)

# COMMAND ----------

# close changed active rows
dt_cancellation_reason.alias("tcr").\
    merge(
        source = df_cancellation_reason_lookup.alias("scr"),
        condition = "tcr.code = scr.code AND tcr.end_date IS NULL AND tcr.description != scr.description"
    ).whenMatchedUpdate(
        set = {
            "tcr.end_date": lit(end_timestamp).cast(TimestampType())
        }
    ).execute()       

# COMMAND ----------

# insert new current versions
# new rows are inserted for keys we just closed and new keys

# list of keys we just closed
closed_codes = [row.code for row in dt_cancellation_reason.toDF().\
    filter(f"end_date = '{end_timestamp}'").\
    select("code").\
    collect()]

if len(closed_codes) == 0:
    print("No updated records to insert")
else:
    dt_cancellation_reason.alias("tcr").\
        merge(
            source = df_cancellation_reason_lookup.alias("scr"),
            condition = f"tcr.code not in ({', '.join(map(str, closed_codes))})"
        ).whenNotMatchedInsert(
            values = {
                "code": "scr.code",
                "description": "scr.description",
                "effective_date": current_timestamp(),
                "end_date": lit(None).cast(TimestampType())
            }
        ).execute()

# COMMAND ----------

# insert new keys
dt_cancellation_reason.alias("tcr").\
    merge(
        source = df_cancellation_reason_lookup.alias("scr"),
        condition = "tcr.code = scr.code"
    ).whenNotMatchedInsert(
        values = {
            "code": "scr.code",
            "description": "scr.description",
            "effective_date": current_timestamp(),
            "end_date": lit(None).cast(TimestampType())
        }
    ).execute()