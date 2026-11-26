# Databricks notebook source
# 1️⃣ Widgets
# =====================================================

import json
from pyspark.sql.functions import current_timestamp

start_time = spark.sql("SELECT current_timestamp()").collect()[0][0]

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

# Parse JSON safely
table_metadata = json.loads(dbutils.widgets.get("table_metadata").replace("'", '"'))
table_parameters = json.loads(dbutils.widgets.get("table_parameters").replace("'", '"'))

print("Table Metadata:", table_metadata)
print("Table Parameters:", table_parameters)
print(f"Run ID: {run_id}")


# COMMAND ----------

# 2️⃣ Extract variables from table_metadata & table_parameters
# =====================================================

table_id = int(table_metadata["table_id"])
table_name = table_metadata["table_name"]
source_system = table_metadata["source_system"]
source_schema = table_metadata["source_schema"]
source_table = table_metadata["source_table"]
source_path = table_metadata["source_path"]
bronze_schema = table_metadata["bronze_schema"]

load_type = table_parameters["load_type"]
watermark_column = table_parameters["watermark_column"]

# Build bronze table full qualified name (FQN)
bronze_table_fqn = f"banking.{bronze_schema}.{table_name}"

print(f"table_id         : {table_id}")
print(f"table_name       : {table_name}")
print(f"source_system    : {source_system}")
print(f"source_schema    : {source_schema}")
print(f"source_table     : {source_table}")
print(f"source_path      : {source_path}")
print(f"bronze_schema    : {bronze_schema}")
print(f"load_type        : {load_type}")
print(f"watermark_column : {watermark_column}")
print(f"Target bronze table : {bronze_table_fqn}")

# COMMAND ----------

# DBTITLE 1,Metadata Entry
# COMMAND ----------

# DBTITLE 1,Metadata Entry
# =====================================================
# Make and entry to audit table
# =====================================================
entry_exists = spark.sql(f"""
    SELECT 1
    FROM banking.metadata.pipeline_runs
    WHERE run_id = {run_id} AND table_id = {table_id}
""").count() > 0

if entry_exists:
    spark.sql(f"""
        UPDATE banking.metadata.pipeline_runs
        SET
            layer = 'Bronze',
            run_status = 'INPROGRESS',
            started_at = TIMESTAMP('{start_time}'),
            ended_at = NULL,
            rows_read = NULL,
            rows_written = NULL,
            error_message = NULL
        WHERE run_id = {run_id} AND table_id = {table_id}
    """)
else:
    spark.sql(f"""
        INSERT INTO banking.metadata.pipeline_runs
            (run_id, table_id, layer, run_status, started_at, ended_at,
             rows_read, rows_written, error_message, created_at)
        VALUES (
            {run_id},
            {table_id},
            'Bronze',
            'INPROGRESS',
            TIMESTAMP('{start_time}'),
            NULL,  -- end time
            NULL,  -- rows read
            NULL,  -- rows written
            NULL,  -- error message
            current_timestamp()
        )
    """)

# COMMAND ----------

# COMMAND ----------

# 3️⃣ Get Last Watermark (For Filtering Only)
# =====================================================

last_watermark = None

if load_type in ["APPEND", "MERGE"] and watermark_column:
    watermark_df = spark.sql(f"""
        SELECT last_watermark_value
        FROM banking.metadata.table_watermarks
        WHERE table_id = {table_id}
    """)
    
    if watermark_df.count() > 0:
        last_watermark = watermark_df.first()["last_watermark_value"]

print("Last Watermark:", last_watermark)

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists banking.bronze

# COMMAND ----------

# DBTITLE 1,Read Source & Load to Bronze
# 4️⃣ Read Source
# =====================================================

try:
    if source_system == "sqlserver":

        # 🔐 Read connection JSON from secret
        secret_json = dbutils.secrets.get(
            scope="banking-scope",
            key="sqlserver-connection-json"
        )

        config = json.loads(secret_json)

        jdbc_url = f"jdbc:sqlserver://{config['host']}:{config['port']};database={config['database']}"

        jdbc_properties = {
            "user": config["user"],
            "password": config["password"],
            "driver": config["driver"]
        }

        # Build query
        if load_type in ["APPEND", "MERGE"] and last_watermark:
            query = f"""
            (SELECT * FROM {source_schema}.{source_table}
             WHERE {watermark_column} > '{last_watermark}') AS src
            """
        else:
            query = f"(SELECT * FROM {source_schema}.{source_table}) AS src"

        source_df = spark.read.jdbc(
            url=jdbc_url,
            table=query,
            properties=jdbc_properties
        )

    elif source_system == "blob":

        source_df = (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option(
                "cloudFiles.schemaLocation",
                f"/Volumes/banking/source/volume/_schema/{table_name}"
            )
            .option("header", "true")
            .load(source_path)
        )

    else:
        raise ValueError("Unsupported source_system")

    # =====================================================
    # 5️⃣ Add insert_timestamp
    # =====================================================

    source_df = source_df.withColumn("insert_timestamp", current_timestamp())

    # =====================================================
    # 6️⃣ Write to Bronze (Append Only)
    # =====================================================

    if source_system == "blob":

        (
            source_df.writeStream
            .format("delta")
            .option(
                "checkpointLocation",
                f"/Volumes/banking/source/volume/_checkpoints/{table_name}"
            )            
            .outputMode("append")
            .trigger(availableNow=True)
            .toTable(bronze_table_fqn)
        )

        records_read = None  # Streaming, can't count records here

    else:

        (
            source_df.write
            .format("delta")
            .mode("append")
            .saveAsTable(bronze_table_fqn)
        )

        records_read = source_df.count()

    print("Source → Bronze Load Completed Successfully.")
    print("Watermark will be updated after Silver load.")

except Exception as e:
    end_time = spark.sql("SELECT current_timestamp()").collect()[0][0]
    error_message = str(e)

    spark.sql(f"""
        UPDATE banking.metadata.pipeline_runs
        SET
            ended_at = TIMESTAMP('{end_time}'),
            run_status = 'FAILED',
            error_message = {'NULL' if not error_message else "'" + error_message.replace("'", "") + "'"}
        WHERE table_id = {table_id} AND run_id = {run_id} 
    """)
    raise


# COMMAND ----------



# COMMAND ----------

