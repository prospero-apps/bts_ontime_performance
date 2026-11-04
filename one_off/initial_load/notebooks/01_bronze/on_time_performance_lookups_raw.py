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

from modules.initialization.lookup_initializer import initialize_lookup_table

# COMMAND ----------

# DBTITLE 1,Load carriers lookup
initialize_lookup_table("L_UNIQUE_CARRIERS", "carriers_lookup")

# COMMAND ----------

initialize_lookup_table("L_STATE_ABR_AVIATION", "state_lookup")

# COMMAND ----------

initialize_lookup_table("L_AIRPORT", "airport_lookup")

# COMMAND ----------

initialize_lookup_table("L_CANCELLATION", "cancellation_reason_lookup")