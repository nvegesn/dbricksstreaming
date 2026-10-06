# Databricks notebook source
# MAGIC %md
# MAGIC # Starbucks Real-Time Store Operations Streaming Pipeline
# MAGIC **Target:** Databricks Community Edition / Free Tier & Single-Node Compute
# MAGIC 
# MAGIC Ingests live order telemetry from Confluent Cloud Kafka, applies schema evolution via Confluent Schema Registry, eliminates Times Square data skew via Two-Stage Salting, and writes to Delta Lake Bronze & Silver layers.

# COMMAND ----------

import os
import sys
import importlib

# Ensure repository root is in Python sys.path when running from Databricks Git Folders
notebook_dir = os.getcwd()
repo_root = os.path.dirname(notebook_dir) if "notebooks" in notebook_dir else notebook_dir
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

# Force eviction of cached Python modules so git pull changes apply immediately
for mod_name in list(sys.modules.keys()):
    if mod_name.startswith("src.") or mod_name == "src":
        sys.modules.pop(mod_name, None)

print(f"Active Workspace Root: {repo_root}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Load Configuration
# MAGIC Selects `community_edition_config.json` for DBFS storage paths and single-node tuning parameters.

# COMMAND ----------

import src.starbucks_store_operations_pipeline as _sbux_pipeline
importlib.reload(_sbux_pipeline)

from pyspark.sql.functions import col
from src.starbucks_store_operations_pipeline import (
    load_pipeline_config,
    init_spark_session,
    init_unity_catalog_assets,
    get_dbutils,
    build_kafka_source,
    write_bronze_stream,
    process_and_write_silver_stream,
    StarbucksStreamMetricsListener
)

# Optional interactive widgets (fills from env vars or allows manual entry)
try:
    dbutils = get_dbutils(spark)
    if dbutils:
        dbutils.widgets.dropdown("ENVIRONMENT", "serverless", ["serverless", "community_edition"], "0. Target Environment")
        dbutils.widgets.text("KAFKA_BOOTSTRAP_SERVERS", os.getenv("KAFKA_BOOTSTRAP_SERVERS", ""), "1. Kafka Bootstrap Server")
        dbutils.widgets.text("KAFKA_API_KEY", os.getenv("KAFKA_API_KEY", ""), "2. Kafka API Key")
        dbutils.widgets.text("KAFKA_API_SECRET", os.getenv("KAFKA_API_SECRET", ""), "3. Kafka API Secret")
        dbutils.widgets.text("SCHEMA_REGISTRY_URL", os.getenv("SCHEMA_REGISTRY_URL", ""), "4. Schema Registry URL")
        dbutils.widgets.text("SCHEMA_REGISTRY_API_KEY", os.getenv("SCHEMA_REGISTRY_API_KEY", ""), "5. SR API Key")
        dbutils.widgets.text("SCHEMA_REGISTRY_API_SECRET", os.getenv("SCHEMA_REGISTRY_API_SECRET", ""), "6. SR API Secret")
except Exception:
    pass

selected_env = "serverless"
try:
    if dbutils:
        selected_env = dbutils.widgets.get("ENVIRONMENT") or "serverless"
except Exception:
    pass

cfg_filename = "serverless_config.json" if selected_env == "serverless" else "community_edition_config.json"
config_path = os.path.join(repo_root, "config", cfg_filename)
config = load_pipeline_config(config_path)
print(f"Loaded Configuration for Environment: {config.get('environment')} (from {cfg_filename})")

# If Serverless, explicitly provision Unity Catalog Schema, Checkpoints Volume, and Delta Tables
if selected_env == "serverless":
    init_unity_catalog_assets(spark, config)
    display(spark.sql("SHOW TABLES IN main.default"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Attach Operational Telemetry & Initialize Pipeline

# COMMAND ----------

dbutils = get_dbutils(spark)

# Register query progress listener
spark.streams.addListener(StarbucksStreamMetricsListener())

print(">> Building Kafka Streaming Source...")
kafka_raw_stream = build_kafka_source(spark, dbutils, config)

print(">> Starting Bronze Audit Layer Stream...")
bronze_query = write_bronze_stream(kafka_raw_stream, config)
print(f"Bronze Stream ID: {bronze_query.id}")

print(">> Starting Silver Curated Metrics Stream (Watermarked & Two-Stage Salted)...")
silver_query = process_and_write_silver_stream(kafka_raw_stream, dbutils, config)
print(f"Silver Stream ID: {silver_query.id}")

# On Serverless (availableNow mode), await batch completion
is_available_now = config.get("tuning_parameters", {}).get("trigger_available_now", False)
if is_available_now:
    print(">> Serverless availableNow trigger active. Waiting for micro-batch to finish processing...")
    bronze_query.awaitTermination()
    silver_query.awaitTermination()
    print(">> Micro-batch processed successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Live Dashboard Monitor (Real-Time Metrics Query)
# MAGIC Run below cell to inspect live 5-minute rolling Average Order Value (AOV) and Barista Wait Times per store.

# COMMAND ----------

silver_table_name = config.get("delta_lake", {}).get("silver_table_name")
silver_path = config.get("delta_lake", {}).get("silver_table_path")

if silver_table_name:
    df_silver = spark.table(silver_table_name)
else:
    df_silver = spark.read.format("delta").load(silver_path)

display(
    df_silver.orderBy(col("window_end").desc(), col("avg_wait_time_seconds").desc())
)
