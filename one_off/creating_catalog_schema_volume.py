# Databricks notebook source
# create catalog
spark.sql("create catalog if not exists bts_ontime")

# COMMAND ----------

# create schemas
spark.sql("create schema if not exists bts_ontime.00_landing")
spark.sql("create schema if not exists bts_ontime.01_bronze")
spark.sql("create schema if not exists bts_ontime.02_silver")
spark.sql("create schema if not exists bts_ontime.03_gold")

# COMMAND ----------

# create volume
spark.sql("create volume if not exists bts_ontime.00_landing.data_sources")