# Databricks notebooks

Databricks **SOURCE**-format notebooks (`.py`) for the Binance medallion pipeline. These are byte-for-byte copies of the notebooks exported from the workspace folder `/Workspace/consolidated_pipeline`.

## Layout

| Repo file | Workspace path | Layer / purpose |
|---|---|---|
| `notebooks/00_setup/00. Setup.py` | `/Workspace/consolidated_pipeline/00. Setup` | Creates catalog `binance_data` and schemas `bronze`, `silver`, `gold_analytics`, `gold_ai`, `monitoring` |
| `notebooks/00_setup/00.configure.py` | `/Workspace/consolidated_pipeline/00.configure` | Shared S3 paths, schema paths, checkpoint paths |
| `notebooks/01_bronze/01.bronze_ingestion.py` | `/Workspace/consolidated_pipeline/01.bronze_ingestion` | Auto Loader ingestion into `bronze.historical_klines_raw`, `bronze.rest_klines_raw`, `bronze.streaming_trades_raw` |
| `notebooks/02_silver/02.silver_trades.py` | `/Workspace/consolidated_pipeline/02.silver_trades` | Dedupe/validate trades into `silver.market_trades`; rejects to `silver.quarantine_trades` |
| `notebooks/02_silver/03.silver_klines.py` | `/Workspace/consolidated_pipeline/03.silver_klines` | Normalize historical + REST klines into `silver.market_klines`; rejects to `silver.quarantine_klines` |
| `notebooks/03_gold/04.gold_analytics.py` | `/Workspace/consolidated_pipeline/04.gold_analytics` | Star schema: `gold_analytics.dim_date`, `dim_time`, `dim_symbol`, `dim_interval`, `fact_market_klines`, `fact_market_trades` (full refresh) |
| `notebooks/03_gold/05.gold_ai_serving.py` | `/Workspace/consolidated_pipeline/05.gold_ai_serving` | AI-serving tables: `gold_ai.market_1m`, `market_indicators`, `market_activity` (full refresh) |
| `notebooks/04_quality/06.data_quality_checks.py` | `/Workspace/consolidated_pipeline/06.data_quality_checks` | Silver/Gold quality assertions and validation queries |
| `notebooks/recovery/04.gold_analytics_recovery_v1.py` | `/Workspace/consolidated_pipeline/recovery/04.gold_analytics_recovery_v1` | Non-destructive full refresh into `gold_analytics_recovery_v1`; omits the Silver `%run` cells |
| `notebooks/recovery/05.gold_ai_recovery_v1.py` | `/Workspace/consolidated_pipeline/recovery/05.gold_ai_recovery_v1` | Non-destructive full refresh into `gold_ai_recovery_v1` |

## Airflow job mapping

`airflow/dags/binance_end_to_end.py` triggers Databricks jobs via Airflow Variables:

| Airflow task | Airflow Variable | Notebook |
|---|---|---|
| `databricks_bronze` | `DATABRICKS_BRONZE_JOB_ID` | `01.bronze_ingestion` |
| `databricks_silver_klines` | `DATABRICKS_SILVER_KLINES_JOB_ID` | `03.silver_klines` |
| `databricks_silver_trades` | `DATABRICKS_SILVER_TRADES_JOB_ID` | `02.silver_trades` |
| `databricks_gold_analytics` | `DATABRICKS_GOLD_ANALYTICS_JOB_ID` | `04.gold_analytics` |
| `databricks_gold_ai` | `DATABRICKS_GOLD_AI_JOB_ID` | `05.gold_ai_serving` |
| `databricks_data_quality` | `DATABRICKS_QUALITY_JOB_ID` | `06.data_quality_checks` |

The recovery jobs are documented in `docs/streaming_checkpoint_recovery.md`:

- `binance_gold_analytics_recovery_v1`: job ID `625607989294712`
- `binance_gold_ai_recovery_v1`: job ID `115647081691022`

## Important notes

- The repo folder structure is for organization only. The notebooks use **absolute** `%run` paths such as `%run /Workspace/consolidated_pipeline/01.bronze_ingestion`. To run them without changes, import/deploy them back to the same workspace paths shown in the table.
- The notebooks are copied exactly as exported. In particular, `03.silver_klines.py` and `06.data_quality_checks.py` contain duplicated blocks that were present in the workspace; they were intentionally not modified.
- `05.gold_ai_serving.py` and `06.data_quality_checks.py` reference `gold_ai.asset_snapshot`, `gold_ai.market_relationships`, `gold_ai.market_alerts`, and `gold_ai.data_dictionary`, but those tables are **not created by any notebook in this export**. They must be produced outside this workspace folder or created separately.
- No credentials or secrets were found in the exported notebooks. Do not add tokens to these files.
