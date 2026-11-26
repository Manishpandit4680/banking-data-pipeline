# Databricks notebook source
import ast

# Create a text widget to accept table metadata (JSON/dict string)
dbutils.widgets.text("table_metadata", "")

# Get the widget value
try:
    table_metadata_str = dbutils.widgets.get("table_metadata")
except Exception as e:
    raise Exception(f"Failed to get table_metadata widget: {e}")

# Print the retrieved value
print(f"table_metadata_str: {table_metadata_str}")

# Validate: widget value cannot be empty
if not table_metadata_str:
    raise ValueError("table_metadata cannot be empty")

# Convert the string to a dictionary using ast
table_metadata = ast.literal_eval(table_metadata_str)

# Extract table_id from the metadata
table_id = table_metadata.get("table_id")

# Validate: table_id must exist and cannot be empty
if not table_id:
    raise ValueError("table_id not found and cannot be empty")

print(f"table_id: {table_id}")

# COMMAND ----------

# 2️⃣ Read Table Parameters
# ==========================================

params_df = (
    spark.table("banking.metadata.table_parameters")
         .filter(f"table_id = {table_id}")
)

# COMMAND ----------

# 3️⃣ Convert to Single JSON Object
# ==========================================

rows = params_df.select("parameter_name", "parameter_value").collect()

parameters_dict = {
    row.parameter_name: row.parameter_value
    for row in rows
}

print("Parameters JSON Object:")
print(parameters_dict)

# COMMAND ----------


dbutils.jobs.taskValues.set(
    key="table_parameters",
    value=parameters_dict
)

print("Task value 'table_parameters' has been set.")
