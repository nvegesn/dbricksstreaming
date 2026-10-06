"""
================================================================================
SCRIPT: starbucks_store_operations_pipeline.py
ROLE: Principal Data Engineer
TARGET RUNTIME: Databricks Runtime (DBR) 13.3 LTS or higher (Spark 3.5+)

BUSINESS CONTEXT:
  Powers the "Starbucks Real-Time Store Operations Dashboard" during peak hours
  (e.g., 7:30 AM - 9:00 AM rush). Ingests in-store POS and Mobile App orders to
  calculate 5-minute rolling Average Order Value (AOV) and Order Wait Times per store.

KEY ARCHITECTURAL HIGHLIGHTS:
  1. Confluent Schema Registry + native from_avro with auto-restart schema evolution.
  2. Ingestion backpressure via maxOffsetsPerTrigger & minPartitions parallelism.
  3. Two-stage aggregation with dynamic salting to eliminate Times Square data skew.
  4. 15-minute event-time watermark to tolerate subway commuter late arrivals.
  5. End-to-end exactly-once semantics backed by Delta Lake ACID logs & RocksDB.
================================================================================
"""

import sys
import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    pmod,
    hash as spark_hash,
    lit,
    sum as _sum,
    count as _count,
    round as _round,
    window,
    coalesce,
    unix_timestamp
)
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.streaming import StreamingQueryListener


class StarbucksStreamMetricsListener(StreamingQueryListener):
    """
    Production telemetry listener emitting streaming progress metrics for
    Databricks Lakehouse Monitoring, Datadog, or cloud monitoring sinks.
    """
    def onQueryStarted(self, event):
        print(f"[STREAM START] Query: {event.name} (ID: {event.id}, RunId: {event.runId})")

    def onQueryProgress(self, event):
        progress = event.progress
        batch_id = progress.batchId
        num_records = progress.numInputRows
        rows_per_sec = progress.processedRowsPerSecond
        watermark = progress.eventTime.get("watermark")
        print(
            f"[PROGRESS] Query: {progress.name} | Batch: {batch_id} | "
            f"Input Rows: {num_records} | Throughput: {rows_per_sec:.1f} rows/s | Watermark: {watermark}"
        )

    def onQueryTerminated(self, event):
        if event.exception:
            print(f"[STREAM ERROR] Query terminated with error: {event.exception}")
        else:
            print(f"[STREAM TERMINATED] Query cleanly terminated: {event.id}")


def load_pipeline_config(config_path=None):
    """
    Loads JSON pipeline configuration from file or defaults.
    """
    default_config_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "config",
        "pipeline_config.json"
    )
    resolved_path = config_path or default_config_path
    
    if os.path.exists(resolved_path):
        with open(resolved_path, "r") as f:
            return json.load(f)
    
    # Fallback default configuration structure
    return {
        "confluent_kafka": {
            "secret_scope": "starbucks-confluent-scope",
            "bootstrap_servers_key": "kafka-bootstrap-servers",
            "api_key_ref": "kafka-api-key",
            "api_secret_ref": "kafka-api-secret",
            "topic": "starbucks_live_orders",
            "starting_offsets": "latest",
            "fail_on_data_loss": "false"
        },
        "schema_registry": {
            "secret_scope": "starbucks-confluent-scope",
            "url_key": "schema-registry-url",
            "api_key_ref": "schema-registry-api-key",
            "api_secret_ref": "schema-registry-api-secret",
            "subject": "starbucks_live_orders-value",
            "avro_schema_evolution_mode": "restart",
            "mode": "PERMISSIVE"
        },
        "delta_lake": {
            "bronze_table_path": "abfss://bronze@starbuckslakehouse.dfs.core.windows.net/orders/bronze_live_orders",
            "bronze_checkpoint_path": "abfss://checkpoints@starbuckslakehouse.dfs.core.windows.net/orders/bronze_live_orders_ckpt",
            "silver_table_path": "abfss://silver@starbuckslakehouse.dfs.core.windows.net/orders/silver_store_operations_metrics",
            "silver_checkpoint_path": "abfss://checkpoints@starbuckslakehouse.dfs.core.windows.net/orders/silver_store_operations_metrics_ckpt"
        },
        "tuning_parameters": {
            "max_offsets_per_trigger": 50000,
            "min_partitions": 64,
            "salt_factor": 16,
            "watermark_duration": "15 minutes",
            "window_duration": "5 minutes",
            "slide_duration": "5 minutes",
            "trigger_processing_time": "10 seconds",
            "spark_shuffle_partitions": 128
        }
    }


def get_dbutils(spark):
    """
    Safely retrieves the Databricks DBUtils instance without breaking local execution.
    """
    try:
        from pyspark.dbutils import DBUtils
        return DBUtils(spark)
    except ImportError:
        try:
            import IPython
            return IPython.get_ipython().user_ns.get("dbutils")
        except Exception:
            return None


def init_spark_session(config):
    """
    Initializes SparkSession with production state-store and shuffle optimizations.
    """
    tuning = config.get("tuning_parameters", {})
    shuffle_partitions = str(tuning.get("spark_shuffle_partitions", 128))

    spark = SparkSession.builder \
        .appName("Starbucks_Realtime_Store_Operations") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.shuffle.partitions", shuffle_partitions) \
        .getOrCreate()

    # ENTERPRISE STATE MANAGEMENT:
    # Use RocksDB StateStoreProvider to offload multi-gigabyte aggregation state
    # from JVM Heap to off-heap memory and local worker NVMe SSDs.
    spark.conf.set(
        "spark.sql.streaming.stateStore.providerClass",
        "com.databricks.sql.streaming.state.RocksDBStateStoreProvider"
    )
    # Changelog checkpointing creates lightweight delta logs for state updates,
    # cutting checkpoint pause times by up to 80% during morning peak bursts.
    spark.conf.set(
        "spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled",
        "true"
    )

    return spark


def get_credential(spark, dbutils, scope, key, env_var, default=""):
    """
    Production-grade multi-tier credential resolver:
    Tier 1 (Enterprise / Commercial): Databricks Secret Scope via dbutils.secrets
    Tier 2 (Interactive / Community Edition): Databricks Notebook Widgets (dbutils.widgets)
    Tier 3 (Cluster / Container): Environment Variables (os.environ)
    Tier 4 (Alternative / Spark Conf): Spark Session Configuration (spark.conf)
    """
    if dbutils:
        # Tier 1: Secret Scope
        try:
            val = dbutils.secrets.get(scope=scope, key=key)
            if val:
                return val
        except Exception:
            pass  # Fall through gracefully in Community Edition where Secret Scopes are restricted

        # Tier 2: Interactive Widgets
        try:
            val = dbutils.widgets.get(env_var)
            if val and str(val).strip():
                return str(val).strip()
        except Exception:
            pass

        try:
            val = dbutils.widgets.get(key)
            if val and str(val).strip():
                return str(val).strip()
        except Exception:
            pass

    # Tier 3: Environment Variables
    env_val = os.getenv(env_var)
    if env_val:
        return env_val

    # Tier 4: Spark Session Configuration
    try:
        conf_val = spark.conf.get(f"spark.secrets.{key}")
        if conf_val:
            return conf_val
    except Exception:
        pass

    return default


def build_kafka_source(spark, dbutils, config):
    """
    Builds the Kafka streaming DataFrame with SASL_SSL authentication and backpressure throttling.
    """
    kafka_conf = config["confluent_kafka"]
    tuning_conf = config["tuning_parameters"]
    secret_scope = kafka_conf["secret_scope"]

    bootstrap_servers = get_credential(
        spark, dbutils, secret_scope,
        kafka_conf["bootstrap_servers_key"], "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
    )
    api_key = get_credential(
        spark, dbutils, secret_scope,
        kafka_conf["api_key_ref"], "KAFKA_API_KEY", ""
    )
    api_secret = get_credential(
        spark, dbutils, secret_scope,
        kafka_conf["api_secret_ref"], "KAFKA_API_SECRET", ""
    )

    jaas_config = (
        f"org.apache.kafka.common.security.plain.PlainLoginModule required "
        f"username='{api_key}' "
        f"password='{api_secret}';"
    )

    # BACKPRESSURE CONFIGURATION:
    # 1. maxOffsetsPerTrigger: Enforces a ceiling on records per micro-batch (e.g. 50,000).
    #    When 10,000 stores simultaneously open at 7:30 AM, backlog surges are smoothed
    #    into bounded micro-batches rather than crashing executors with OutOfMemoryError.
    # 2. minPartitions: Divides Kafka partition data across more Spark tasks (64)
    #    than physical Kafka partitions, saturating all cluster CPU cores.
    # 3. failOnDataLoss: Prevents streaming pipeline failure if Kafka retention purges
    #    un-consumed offsets during prolonged upstream network outages.
    kafka_options = {
        "kafka.bootstrap.servers": bootstrap_servers,
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": jaas_config,
        "subscribe": kafka_conf["topic"],
        "startingOffsets": kafka_conf.get("starting_offsets", "latest"),
        "maxOffsetsPerTrigger": str(tuning_conf.get("max_offsets_per_trigger", 50000)),
        "minPartitions": str(tuning_conf.get("min_partitions", 64)),
        "failOnDataLoss": kafka_conf.get("fail_on_data_loss", "false")
    }

    return spark.readStream.format("kafka").options(**kafka_options).load()


def init_unity_catalog_assets(spark, config):
    """
    Explicitly provisions Unity Catalog Schema, Volume for streaming checkpoints,
    and Delta Bronze & Silver tables if Unity Catalog names are configured.
    """
    delta_conf = config.get("delta_lake", {})
    catalog = delta_conf.get("catalog", "main")
    database = delta_conf.get("database", "default")
    bronze_table = delta_conf.get("bronze_table_name")
    silver_table = delta_conf.get("silver_table_name")

    print(f">> Initializing Unity Catalog assets in {catalog}.{database}...")

    # 1. Provision Schema / Database
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{database}")
        print(f"Verified Schema: {catalog}.{database}")
    except Exception as e:
        print(f"Notice during CREATE SCHEMA: {e}")

    # 2. Provision Volume for Streaming Checkpoints
    try:
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{database}.starbucks_checkpoints")
        print(f"Verified Checkpoints Volume: {catalog}.{database}.starbucks_checkpoints")
    except Exception as e:
        print(f"Notice during CREATE VOLUME: {e}")

    # 3. Explicitly Provision Bronze Delta Table
    if bronze_table:
        try:
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {bronze_table} (
                    kafka_key STRING COMMENT 'Kafka message partition key (store_id)',
                    raw_payload BINARY COMMENT 'Raw Confluent Avro wire payload with 5-byte header',
                    topic STRING COMMENT 'Source Kafka topic name',
                    partition INT COMMENT 'Kafka topic partition number',
                    offset BIGINT COMMENT 'Monotonically increasing Kafka partition offset',
                    kafka_published_timestamp TIMESTAMP COMMENT 'Kafka broker publishing timestamp',
                    bronze_ingestion_timestamp TIMESTAMP COMMENT 'Lakehouse ingestion timestamp'
                )
                USING DELTA
                COMMENT 'Bronze Layer: Immutable audit log of raw Starbucks order events'
                TBLPROPERTIES (
                    'delta.enableChangeDataFeed' = 'true',
                    'delta.autoOptimize.optimizeWrite' = 'true',
                    'delta.autoOptimize.autoCompact' = 'true'
                )
            """)
            print(f"Verified Bronze Delta Table DDL: {bronze_table}")
        except Exception as e:
            print(f"Notice during Bronze CREATE TABLE: {e}")

    # 4. Explicitly Provision Silver Delta Table
    if silver_table:
        try:
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {silver_table} (
                    window_start TIMESTAMP COMMENT 'Start boundary of 5-minute aggregation window',
                    window_end TIMESTAMP COMMENT 'End boundary of 5-minute aggregation window',
                    store_id STRING COMMENT 'Store identifier e.g. STORE-NYC-7381',
                    total_order_count BIGINT COMMENT 'Total orders processed within 5-minute window',
                    avg_order_value DOUBLE COMMENT 'Average Order Value (AOV) in USD calculated via exact algebraic rollup',
                    avg_wait_time_seconds DOUBLE COMMENT 'Average fulfillment wait time in seconds',
                    metrics_calculated_timestamp TIMESTAMP COMMENT 'Timestamp when metrics calculation was committed'
                )
                USING DELTA
                PARTITIONED BY (store_id)
                COMMENT 'Silver Layer: Real-time store performance metrics with Times Square salting'
                TBLPROPERTIES (
                    'delta.autoOptimize.optimizeWrite' = 'true',
                    'delta.autoOptimize.autoCompact' = 'true'
                )
            """)
            print(f"Verified Silver Delta Table DDL: {silver_table}")
        except Exception as e:
            print(f"Notice during Silver CREATE TABLE: {e}")


def write_bronze_stream(kafka_raw_df, config):
    """
    Writes raw event stream to Delta Bronze table for immutable auditability and replay.
    """
    delta_conf = config["delta_lake"]
    tuning_conf = config["tuning_parameters"]

    bronze_df = kafka_raw_df.select(
        col("key").cast("string").alias("kafka_key"),
        col("value").alias("raw_payload"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_published_timestamp"),
        current_timestamp().alias("bronze_ingestion_timestamp")
    )

    # EXACTLY-ONCE GUARANTEE:
    # OutputMode("append") + checkpointLocation records micro-batch offsets.
    # Delta Lake ensures idempotent atomic commits into _delta_log.
    writer = (
        bronze_df.writeStream
        .queryName("Starbucks_Bronze_Ingest")
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", delta_conf["bronze_checkpoint_path"])
        .option("mergeSchema", "true")
    )

    if tuning_conf.get("trigger_available_now", False):
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=tuning_conf.get("trigger_processing_time", "10 seconds"))

    table_name = delta_conf.get("bronze_table_name")
    if table_name:
        return writer.toTable(table_name)
    return writer.start(delta_conf["bronze_table_path"])


def process_and_write_silver_stream(kafka_raw_df, dbutils, config):
    """
    Parses Avro via Confluent Schema Registry, applies event-time watermark,
    executes two-stage salting aggregation to defeat data skew, and writes to Delta Silver.
    """
    sr_conf = config["schema_registry"]
    delta_conf = config["delta_lake"]
    tuning_conf = config["tuning_parameters"]
    secret_scope = sr_conf["secret_scope"]

    spark = kafka_raw_df.sparkSession
    sr_url = get_credential(
        spark, dbutils, secret_scope,
        sr_conf["url_key"], "SCHEMA_REGISTRY_URL", "http://localhost:8081"
    )
    sr_api_key = get_credential(
        spark, dbutils, secret_scope,
        sr_conf["api_key_ref"], "SCHEMA_REGISTRY_API_KEY", ""
    )
    sr_api_secret = get_credential(
        spark, dbutils, secret_scope,
        sr_conf["api_secret_ref"], "SCHEMA_REGISTRY_API_SECRET", ""
    )

    # SCHEMA EVOLUTION MECHANISM:
    # 1. Confluent Schema Registry stores versioned schemas.
    # 2. avroSchemaEvolutionMode: "restart" instructs from_avro to throw an
    #    UnknownFieldException when a message arrives with a newly registered schema ID.
    # 3. Databricks job orchestrator auto-restarts the query, pulling the latest schema.
    # 4. .option("mergeSchema", "true") on Delta sink writes new columns seamlessly.
    schema_registry_options = {
        "confluent.schema.registry.basic.auth.credentials.source": "USER_INFO",
        "confluent.schema.registry.basic.auth.user.info": f"{sr_api_key}:{sr_api_secret}",
        "avroSchemaEvolutionMode": sr_conf.get("avro_schema_evolution_mode", "restart"),
        "mode": sr_conf.get("mode", "PERMISSIVE")
    }

    # Deserialization of Confluent Avro wire-format payload
    parsed_orders = kafka_raw_df.select(
        from_avro(
            data=col("value"),
            subject=sr_conf["subject"],
            schemaRegistryAddress=sr_url,
            options=schema_registry_options
        ).alias("order_event")
    ).select("order_event.*")

    # LATE-ARRIVING DATA HANDLING (The Subway Commuter):
    # Customer orders at 8:00 AM on mobile app, enters subway tunnel, arrives at Kafka 8:12 AM.
    # 1. We MUST use order_placed_timestamp as event time, NOT Kafka processing time.
    # 2. withWatermark(15 minutes) guarantees state is kept open for 15 minutes.
    # 3. Data older than 15 minutes is safely dropped, bounding RocksDB state store size.
    curated_orders = (
        parsed_orders
        .withColumn("order_placed_timestamp", col("order_placed_timestamp").cast("timestamp"))
        .withColumn("order_fulfilled_timestamp", col("order_fulfilled_timestamp").cast("timestamp"))
        .withColumn("transaction_amount", col("transaction_amount").cast("double"))
        .withColumn(
            "wait_time_seconds",
            coalesce(
                (unix_timestamp("order_fulfilled_timestamp") - unix_timestamp("order_placed_timestamp")),
                lit(0)
            ).cast("double")
        )
        .filter(col("store_id").isNotNull() & col("order_placed_timestamp").isNotNull())
        .withWatermark("order_placed_timestamp", tuning_conf.get("watermark_duration", "15 minutes"))
    )

    # DATA SKEW HANDLING (The Times Square Problem):
    # Times Square (Store #7381) processes 100x orders vs rural stores. Direct grouping
    # hashes all Times Square records to ONE shuffle partition, freezing 1 core for minutes.
    #
    # Two-Stage Salting:
    # 1. Add salt column (0..15). Times Square is partitioned across 16 parallel tasks.
    # 2. Stage 1: Partial aggregates (sum of amount, sum of wait time, count of orders).
    # 3. Stage 2: Roll up partials per (window, store_id). At most 16 rows per store are
    #    shuffled in Stage 2, eliminating stragglers completely.
    #
    # Mathematical Correctness: We NEVER average averages! We decompose into:
    # Avg Order Value = sum(amount) / sum(count)
    # Avg Wait Time   = sum(wait_time) / sum(count)
    salt_factor = tuning_conf.get("salt_factor", 16)
    window_duration = tuning_conf.get("window_duration", "5 minutes")
    slide_duration = tuning_conf.get("slide_duration", "5 minutes")

    salted_orders = curated_orders.withColumn(
        "salt",
        pmod(spark_hash(col("order_id")), lit(salt_factor))
    )

    # Stage 1: Salted Partial Aggregations
    stage1_partial_agg = (
        salted_orders
        .groupBy(
            window(col("order_placed_timestamp"), window_duration, slide_duration),
            col("store_id"),
            col("salt")
        )
        .agg(
            _sum("transaction_amount").alias("partial_sales_amount"),
            _sum("wait_time_seconds").alias("partial_wait_time_seconds"),
            _count(lit(1)).alias("partial_order_count")
        )
    )

    # Stage 2: Global Rollup Aggregation (Spark 3.5+ chained stateful streaming)
    stage2_final_agg = (
        stage1_partial_agg
        .groupBy(
            col("window"),
            col("store_id")
        )
        .agg(
            _sum("partial_sales_amount").alias("total_sales_amount"),
            _sum("partial_wait_time_seconds").alias("total_wait_time_seconds"),
            _sum("partial_order_count").alias("total_order_count")
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("store_id"),
            col("total_order_count"),
            _round(col("total_sales_amount") / col("total_order_count"), 2).alias("avg_order_value"),
            _round(col("total_wait_time_seconds") / col("total_order_count"), 1).alias("avg_wait_time_seconds"),
            current_timestamp().alias("metrics_calculated_timestamp")
        )
    )

    # EXACTLY-ONCE SEMANTICS:
    # Checkpoint records commit logs; Delta Lake commits atomic metadata JSON.
    # If cluster dies, uncommitted temporary writes are discarded; on recovery,
    # the exact Kafka offset batch is re-read without duplicate rows in Silver.
    writer = (
        stage2_final_agg.writeStream
        .queryName("Starbucks_Silver_Metrics")
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", delta_conf["silver_checkpoint_path"])
        .option("mergeSchema", "true")
    )

    if tuning_conf.get("trigger_available_now", False):
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=tuning_conf.get("trigger_processing_time", "10 seconds"))

    table_name = delta_conf.get("silver_table_name")
    if table_name:
        return writer.toTable(table_name)
    return writer.start(delta_conf["silver_table_path"])


def main():
    """
    Main entry point for Databricks Workflow execution.
    """
    config_override_path = sys.argv[1] if len(sys.argv) > 1 else None
    config = load_pipeline_config(config_override_path)
    
    spark = init_spark_session(config)
    dbutils = get_dbutils(spark)

    # Attach operational telemetry listener
    spark.streams.addListener(StarbucksStreamMetricsListener())

    print(">> Initializing Starbucks Real-Time Operations Ingestion Pipeline...")
    kafka_raw_stream = build_kafka_source(spark, dbutils, config)

    print(">> Launching Bronze Lakehouse Stream...")
    bronze_query = write_bronze_stream(kafka_raw_stream, config)

    print(">> Launching Silver Curated Metrics Stream (Watermarked & Salted)...")
    silver_query = process_and_write_silver_stream(kafka_raw_stream, dbutils, config)

    print(">> Streaming queries running. Awaiting termination...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
