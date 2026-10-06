-- =============================================================================
-- SCRIPT: init_unity_catalog.sql
-- PURPOSE: Provision Unity Catalog assets (Schema, Volumes, and Delta Tables)
-- TARGET: Databricks Serverless Compute / Unity Catalog
-- =============================================================================

-- 1. Ensure Catalog and Schema exist
-- In standard workspaces, catalog 'main' is pre-created by default.
CREATE SCHEMA IF NOT EXISTS main.default
COMMENT 'Default workspace schema for real-time analytics';

-- 2. Create Unity Catalog Volume for Structured Streaming Checkpoints
CREATE VOLUME IF NOT EXISTS main.default.starbucks_checkpoints
COMMENT 'Persistent storage for streaming RocksDB checkpoint commit logs';

-- 3. Provision Bronze Delta Table (Raw Ingestion Layer)
CREATE TABLE IF NOT EXISTS main.default.starbucks_bronze_live_orders (
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
);

-- 4. Provision Silver Delta Table (Curated Aggregations Layer)
CREATE TABLE IF NOT EXISTS main.default.starbucks_silver_store_operations_metrics (
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
);
