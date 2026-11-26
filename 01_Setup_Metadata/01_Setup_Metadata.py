# Databricks notebook source
# MAGIC %sql
# MAGIC --CATALOG AND SCHEMA
# MAGIC  CREATE CATALOG IF NOT EXISTS banking;
# MAGIC  CREATE SCHEMA IF NOT EXISTS banking.metadata;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS banking.metadata.tables (
# MAGIC   table_id INT,
# MAGIC   table_name STRING,
# MAGIC   source_system STRING,
# MAGIC   source_schema STRING,
# MAGIC   source_table STRING,
# MAGIC   source_path STRING,
# MAGIC   target_layer STRING,
# MAGIC   bronze_schema STRING,
# MAGIC   silver_schema STRING,
# MAGIC   gold_schema STRING,
# MAGIC   active_flag BOOLEAN,
# MAGIC   load_order INT,
# MAGIC   created_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC --metadata.table_parameters
# MAGIC CREATE TABLE IF NOT EXISTS banking.metadata.table_parameters (
# MAGIC   table_id INT,
# MAGIC   parameter_name STRING,
# MAGIC   parameter_value STRING,
# MAGIC   created_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cell 4
# MAGIC %sql
# MAGIC --metadata.table_watermarks
# MAGIC CREATE TABLE IF NOT EXISTS banking.metadata.table_watermarks (
# MAGIC   table_id INT,
# MAGIC   last_watermark_value STRING,
# MAGIC   last_updated_at TIMESTAMP,
# MAGIC   last_run_id BIGINT
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (table_id);

# COMMAND ----------

# MAGIC %sql
# MAGIC --metadata.pipeline_runs
# MAGIC CREATE TABLE IF NOT EXISTS banking.metadata.pipeline_runs (
# MAGIC   run_id BIGINT,
# MAGIC   table_id INT,
# MAGIC   layer STRING,
# MAGIC   start_time TIMESTAMP,
# MAGIC   end_time TIMESTAMP,
# MAGIC   status STRING,
# MAGIC   number_of_records BIGINT,
# MAGIC   error_message STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (table_id);

# COMMAND ----------

# MAGIC %sql
# MAGIC --create source schema and volume for blob landing
# MAGIC CREATE SCHEMA IF NOT EXISTS banking.source;
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS banking.source.volume;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- INSERT INTO metadata.tables
# MAGIC -- =====================================================
# MAGIC
# MAGIC INSERT INTO banking.metadata.tables VALUES
# MAGIC -- 1. Customers (SQL Server)
# MAGIC (1, 'customers', 'sqlserver', 'banking', 'customers', NULL, 'silver', 'bronze', 'silver', NULL, TRUE, 1, current_timestamp()),
# MAGIC
# MAGIC -- 2. Accounts (SQL Server)
# MAGIC (2, 'accounts', 'sqlserver', 'banking', 'accounts', NULL, 'silver', 'bronze', 'silver', NULL, TRUE, 2, current_timestamp()),
# MAGIC
# MAGIC -- 3. Transactions (SQL Server)
# MAGIC (3, 'transactions', 'sqlserver', 'banking', 'transactions', NULL, 'silver', 'bronze', 'silver', NULL, TRUE, 3, current_timestamp()),
# MAGIC
# MAGIC -- 4. Branches (SQL Server - Full Load)
# MAGIC (4, 'branches', 'sqlserver', 'banking', 'branches', NULL, 'silver', 'bronze', 'silver', NULL, TRUE, 4, current_timestamp()),
# MAGIC
# MAGIC -- 5. Credit Bureau Reports (Blob CSV)
# MAGIC (5, 'credit_bureau_reports', 'blob', NULL, NULL,
# MAGIC  '/Volumes/banking/source/volume/credit_bureau_reports/', 'silver',
# MAGIC  'bronze', 'silver', NULL, TRUE, 5, current_timestamp()),
# MAGIC
# MAGIC -- 6. Payment Gateway Logs (Blob CSV)
# MAGIC (6, 'payment_gateway_logs', 'blob', NULL, NULL,
# MAGIC  '/Volumes/banking/source/volume/payment_gateway_logs/', 'silver',
# MAGIC  'bronze', 'silver', NULL, TRUE, 6, current_timestamp());
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- =====================================================
# MAGIC -- INSERT INTO metadata.table_parameters
# MAGIC -- =====================================================
# MAGIC
# MAGIC INSERT INTO banking.metadata.table_parameters VALUES
# MAGIC
# MAGIC -- ================= CUSTOMERS =================
# MAGIC (1, 'load_type', 'MERGE', current_timestamp()),
# MAGIC (1, 'primary_key', 'customer_id', current_timestamp()),
# MAGIC (1, 'watermark_column', 'updated_at', current_timestamp()),
# MAGIC
# MAGIC -- ================= ACCOUNTS =================
# MAGIC (2, 'load_type', 'MERGE', current_timestamp()),
# MAGIC (2, 'primary_key', 'account_id', current_timestamp()),
# MAGIC (2, 'watermark_column', 'updated_at', current_timestamp()),
# MAGIC
# MAGIC -- ================= TRANSACTIONS =================
# MAGIC (3, 'load_type', 'APPEND', current_timestamp()),
# MAGIC (3, 'primary_key', 'txn_id', current_timestamp()),
# MAGIC (3, 'watermark_column', 'txn_timestamp', current_timestamp()),
# MAGIC
# MAGIC -- ================= BRANCHES =================
# MAGIC (4, 'load_type', 'FULL', current_timestamp()),
# MAGIC (4, 'primary_key', 'branch_code', current_timestamp()),
# MAGIC
# MAGIC -- ================= CREDIT BUREAU REPORTS =================
# MAGIC (5, 'load_type', 'MERGE', current_timestamp()),
# MAGIC (5, 'primary_key', 'customer_id', current_timestamp()),
# MAGIC (5, 'watermark_column', 'bureau_pull_date', current_timestamp()),
# MAGIC
# MAGIC -- ================= PAYMENT GATEWAY LOGS =================
# MAGIC (6, 'load_type', 'APPEND', current_timestamp()),
# MAGIC (6, 'primary_key', 'txn_id', current_timestamp()),
# MAGIC (6, 'watermark_column', 'processed_timestamp', current_timestamp());
# MAGIC
# MAGIC
# MAGIC
# MAGIC -- =====================================================
# MAGIC -- OPTIONAL: INITIALIZE WATERMARK TABLE
# MAGIC -- (Only for INCREMENTAL tables)
# MAGIC -- =====================================================
# MAGIC
# MAGIC INSERT INTO banking.metadata.table_watermarks VALUES
# MAGIC (1, '1900-01-01 00:00:00', current_timestamp(), NULL),
# MAGIC (2, '1900-01-01 00:00:00', current_timestamp(), NULL),
# MAGIC (3, '1900-01-01 00:00:00', current_timestamp(), NULL),
# MAGIC (5, '1900-01-01 00:00:00', current_timestamp(), NULL),
# MAGIC (6, '1900-01-01 00:00:00', current_timestamp(), NULL);
# MAGIC
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC INSERT INTO banking.metadata.tables
# MAGIC VALUES (
# MAGIC     7,
# MAGIC     'customer_360',
# MAGIC     'silver',
# MAGIC     NULL,
# MAGIC     NULL,
# MAGIC     NULL,
# MAGIC     'gold',
# MAGIC     NULL,
# MAGIC     NULL,
# MAGIC     'gold',
# MAGIC     TRUE,
# MAGIC     1,
# MAGIC     current_timestamp()
# MAGIC );
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC INSERT INTO banking.metadata.tables
# MAGIC VALUES
# MAGIC (
# MAGIC 8,
# MAGIC 'branch_performance',
# MAGIC 'silver',
# MAGIC NULL,
# MAGIC NULL,
# MAGIC NULL,
# MAGIC 'gold',
# MAGIC NULL,
# MAGIC NULL,
# MAGIC 'gold',
# MAGIC TRUE,
# MAGIC 2,
# MAGIC current_timestamp()
# MAGIC ),
# MAGIC
# MAGIC (
# MAGIC 9,
# MAGIC 'transaction_channel_summary',
# MAGIC 'silver',
# MAGIC NULL,
# MAGIC NULL,
# MAGIC NULL,
# MAGIC 'gold',
# MAGIC NULL,
# MAGIC NULL,
# MAGIC 'gold',
# MAGIC TRUE,
# MAGIC 3,
# MAGIC current_timestamp()
# MAGIC ),
# MAGIC
# MAGIC (
# MAGIC 10,
# MAGIC 'daily_bank_kpi',
# MAGIC 'silver',
# MAGIC NULL,
# MAGIC NULL,
# MAGIC NULL,
# MAGIC 'gold',
# MAGIC NULL,
# MAGIC NULL,
# MAGIC 'gold',
# MAGIC TRUE,
# MAGIC 4,
# MAGIC current_timestamp()
# MAGIC );
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC
# MAGIC INSERT INTO banking.metadata.tables
# MAGIC VALUES (
# MAGIC     11,
# MAGIC     'risk_customer_summary',
# MAGIC     'silver',
# MAGIC     NULL,
# MAGIC     NULL,
# MAGIC     NULL,
# MAGIC     'gold',
# MAGIC     NULL,
# MAGIC     NULL,
# MAGIC     'gold',
# MAGIC     TRUE,
# MAGIC     1,
# MAGIC     current_timestamp()
# MAGIC );