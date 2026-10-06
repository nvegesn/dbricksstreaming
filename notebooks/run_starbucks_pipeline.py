# Databricks notebook source
# MAGIC %md
# MAGIC # Starbucks Real-Time Store Operations Streaming Pipeline
# MAGIC **Target:** Databricks Community Edition / Free Tier & Single-Node Compute
# MAGIC 
# MAGIC Ingests live order telemetry from Confluent Cloud Kafka, applies schema evolution via Confluent Schema Registry, eliminates Times Square data skew via Two-Stage Salting, and writes to Delta Lake Bronze & Silver layers.

# COMMAND ----------

import os
import sys

# Ensure repository root is in Python sys.path when running from Databricks Git Folders
notebook_dir = os.getcwd()
repo_root = os.path.dirname(notebook_dir) if "notebooks" in notebook_dir else notebook_dir
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

print(f"Active Workspace Root: {repo_root}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Load Configuration
# MAGIC Selects `community_edition_config.json` for DBFS storage paths and single-node tuning parameters.

# COMMAND ----------

from pyspark.sql.functions import col
from src.starbucks_store_operations_pipeline import (
    load_pipeline_config,
    init_spark_session,
    get_dbutils,
    build_kafka_source,
    write_bronze_stream,
    process_and_write_silver_stream,
    StarbucksStreamMetricsListener
)

# Optional interactive widgets for Community Edition (fills from env vars or allows manual entry)
try:
    dbutils = get_dbutils(spark)
    if dbutils:
        dbutils.widgets.text("KAFKA_BOOTSTRAP_SERVERS", os.getenv("KAFKA_BOOTSTRAP_SERVERS", ""), "1. Kafka Bootstrap Server")
        dbutils.widgets.text("KAFKA_API_KEY", os.getenv("KAFKA_API_KEY", ""), "2. Kafka API Key")
        dbutils.widgets.text("KAFKA_API_SECRET", os.getenv("KAFKA_API_SECRET", ""), "3. Kafka API Secret")
        dbutils.widgets.text("SCHEMA_REGISTRY_URL", os.getenv("SCHEMA_REGISTRY_URL", ""), "4. Schema Registry URL")
        dbutils.widgets.text("SCHEMA_REGISTRY_API_KEY", os.getenv("SCHEMA_REGISTRY_API_KEY", ""), "5. SR API Key")
        dbutils.widgets.text("SCHEMA_REGISTRY_API_SECRET", os.getenv("SCHEMA_REGISTRY_API_SECRET", ""), "6. SR API Secret")
except Exception:
    pass

config_path = os.path.join(repo_root, "config", "community_edition_config.json")
config = load_pipeline_config(config_path)
print(f"Loaded Configuration for Environment: {config.get('environment')}")

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

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Live Dashboard Monitor (Real-Time Metrics Query)
# MAGIC Run below cell to inspect live 5-minute rolling Average Order Value (AOV) and Barista Wait Times per store.

# COMMAND ----------

silver_path = config["delta_lake"]["silver_table_path"]
display(
    spark.read.format("delta").load(silver_path)
    .orderBy(col("window_end").desc(), col("avg_wait_time_seconds").desc())
)
