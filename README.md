# banking-data-pipeline

Databricks-based banking data engineering project (Unity Catalog: `banking`).

## Structure

- `01_Setup_Metadata` – catalog/schema setup and metadata tables (tables, table_parameters, table_watermarks, pipeline_runs)
- `02_Source_to_Silver` – secret scope setup, source-to-bronze and bronze-to-silver loads driven by metadata
- `03_Silver_to_Gold` – gold layer builds (customer_360, branch_performance, transaction_channel_summary, daily_bank_kpi, risk_customer_summary)
- `04_Email_Notification` – email notification notebook (in progress)

## Notes

- All notebooks reference the `banking` Unity Catalog and the `banking-scope` secret scope.
- Notebooks pass data between stages via job task values and widgets – no hardcoded paths.
- The SQL Server password is stored in the `banking-scope` secret scope (never in code).
