# Databricks notebook source
# DBTITLE 1,Create source_system widget with valid default
#create widget source_system
dbutils.widgets.text("source_system", "sqlserver")

source_system = dbutils.widgets.get("source_system")

if not source_system:
    raise ValueError("Source_system widget can not be empty.")

print(f"source_system: {source_system}")

# COMMAND ----------

# DBTITLE 1,Read and filter metadata tables
# Read the metadata tables into a DataFrame
import pyspark.sql.functions as F
tables_df = spark.read.table("banking.metadata.tables")

# Filter: active flag is true and source_system matches the widget value (case-insensitive), then order by load_order
filtered_df = (
    tables_df
    .filter(
        (F.col("active_flag") == True)
        & (F.lower(F.col("source_system")) == F.lower(F.lit(source_system)))
    )
    .orderBy(F.col("load_order"))
)

display(filtered_df)

# COMMAND ----------

rows = filtered_df.collect()

tables_list = []

for row in rows:
    table = row.asDict()

    for key, value in table.items():
        table[key] = str(value) if value is not None else ""

    tables_list.append(table)

# COMMAND ----------

# Set task value so downstream tasks in the same job can consume this notebook's output
# (read in another task via dbutils.jobs.taskValues.get(taskKey="<this_task_key>", key="tables_metadata"))

dbutils.jobs.taskValues.set(
    key="tables_metadata",
    value=tables_list
)

print("Task value 'tables_metadata' has been set.")