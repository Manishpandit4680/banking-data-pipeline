# Databricks notebook source
# Databricks notebook source
# =====================================================
# 1️⃣ Imports & Setup
# =====================================================

import json
import time
from pyspark.sql.functions import current_timestamp, col, max as spark_max
from delta.tables import DeltaTable




status = "SUCCESS"
error_message = None
records_read = 0
records_written = 0

# COMMAND ----------

# =====================================================
# 2️⃣ Widgets
# =====================================================

dbutils.widgets.text(
    "table_metadata",
    "{'table_id': '1', 'table_name': 'customers', 'source_system': 'sqlserver', "
    "'source_schema': 'banking', 'source_table': 'customers', "
    "'source_path': '', 'bronze_schema': 'bronze', "
    "'silver_schema': 'silver', 'active_flag': 'True', "
    "'load_order': '1', 'created_at': '2026-02-18 13:23:37.053711'}"
)

dbutils.widgets.text(
    "table_parameters",
    "{'load_type': 'MERGE', "
    "'primary_key': 'customer_id', "
    "'watermark_column': 'updated_at'}"
)

dbutils.widgets.text(
    "run_id",
    "472519629468310"
)
run_id=dbutils.widgets.get("run_id")

table_metadata = json.loads(dbutils.widgets.get("table_metadata").replace("'", '"'))
table_parameters = json.loads(dbutils.widgets.get("table_parameters").replace("'", '"'))


table_id = int(table_metadata["table_id"])
table_name = table_metadata["table_name"]
bronze_schema = table_metadata["bronze_schema"]
silver_schema = table_metadata["silver_schema"]

load_type = table_parameters.get("load_type")
primary_key = table_parameters.get("primary_key")
watermark_column = table_parameters.get("watermark_column")

bronze_table = f"banking.{bronze_schema}.{table_name}"
silver_table = f"banking.{silver_schema}.{table_name}"

print(f"Processing Table: {table_name}")
print(f"Load Type: {load_type}")
print(f"Run ID: {run_id}")

# COMMAND ----------

# COMMAND ----------

