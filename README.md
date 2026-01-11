# pyspark-config-etl-framework
Building a python framework for pyspark.
# PySpark Config-Driven ETL Framework (Delta)

A bank-style, config-driven ETL framework:
- Read input data (parquet/csv/json/delta)
- Apply a dataset-specific transformation function
- Write output to Delta (table or path)
- Supports `partition_by` via JSON config
- Optional required-column checks + de-dup

## Requirements
- Python 3.9+
- PySpark
- Delta Lake (Databricks or open-source delta-spark)

## Install (local)
```bash
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e .

