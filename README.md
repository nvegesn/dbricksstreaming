# Starbucks Real-Time Store Operations Streaming Pipeline

Production-ready Lakehouse Structured Streaming engine ingesting live Starbucks POS and Mobile App order streams from Confluent Kafka into Delta Lake (Bronze & Silver layers).

---

## 1. Project Structure

```
.
├── config/
│   └── pipeline_config.json          # Production cluster & streaming configurations
├── databricks/
│   └── databricks_job.json           # Databricks Workflow job definition with auto-retry
├── schemas/
│   └── starbucks_live_orders_value.avsc # Confluent Avro schema (seasonal modifiers & split tender)
├── src/
│   ├── starbucks_store_operations_pipeline.py # Production PySpark Structured Streaming application
│   └── producer_simulator.py         # Mock telemetry generator (skew & late event testing)
├── tests/
│   └── test_salting_logic.py         # Mathematical equivalence proof for Two-Stage Salting
└── README.md                         # Architecture, deployment guide, and operational runbook
```

---

## 2. Core Architectural Components

### Edge Case 1: Schema Evolution (Confluent Schema Registry + Delta Lake)
* **Avro Wire Format:** Confluent prepends a 5-byte header (`0x00` magic byte + 4-byte big-endian Schema ID) to every payload.
* **`from_avro` Integration:** Configured with `avroSchemaEvolutionMode: "restart"`. When menu modifications (such as seasonal syrup modifiers or new split-tender payment types) introduce a new schema ID, the stream catches `UnknownFieldException`.
* **Automated Recovery:** Databricks Workflows restarts the task, dynamically re-fetching the updated schema from Confluent Schema Registry.
* **Delta `mergeSchema`:** `.option("mergeSchema", "true")` applies an atomic schema migration (`ADD COLUMNS`) to the target Delta Lake table without downtime or manual DDL.

### Edge Case 2: Backpressure & Rate Limiting (The Morning Rush)
* **Rate Throttling:** `maxOffsetsPerTrigger: 50000` enforces a bounded micro-batch ceiling during the 7:30 AM – 9:00 AM rush, preventing executor heap exhaustion and GC pauses.
* **Partition Saturation:** `minPartitions: 64` subdivides Kafka topic partitions across all cluster vCPUs to maximize parallel core utilization.
* **Fault Tolerance:** `failOnDataLoss: "false"` guards against stream termination if Kafka retention policies expire offsets during upstream outages.

### Edge Case 3: Data Skew Handling (The Times Square Problem)
* **The Problem:** Times Square (Store #7381) processes 100x the volume of typical stores. Direct `groupBy(store_id)` creates a straggler task where a single core handles all Times Square records.
* **Two-Stage Salting:**
  1. Records are salted: `salt = pmod(hash(order_id), 16)`.
  2. **Stage 1 (Partial):** Group by `(window, store_id, salt)` and compute partial sums and counts across 16 parallel tasks.
  3. **Stage 2 (Global):** Group by `(window, store_id)` and roll up partial sums and counts into exact algebraic averages ($AOV = \frac{\sum amount}{\sum count}$).

### Edge Case 4: Late-Arriving Data (The Subway Commuter)
* **Event Time:** Uses `order_placed_timestamp` embedded in the payload, never Kafka's ingestion timestamp.
* **Watermark:** `.withWatermark("order_placed_timestamp", "15 minutes")` keeps state windows open for up to 15 minutes, allowing mobile orders placed in subway dead-zones to be correctly aggregated upon reconnect.
* **State Eviction:** Data older than 15 minutes is safely dropped to bound the RocksDB state store size.

### Edge Case 5: Exactly-Once Processing
* **WAL Checkpointing:** Structured Streaming maintains `offsets/` and `commits/` in cloud storage.
* **Delta Lake ACID Transactions:** Delta writes record the streaming query ID and batch ID atomically in `_delta_log/`. If a cluster crashes mid-batch, uncommitted Parquet files are ignored, and re-executed batches are idempotent.

---

## 3. Deployment & Execution Guide

### Step 1: Provision Databricks Secret Scope
```bash
databricks secrets create-scope --scope starbucks-confluent-scope
databricks secrets put-secret --scope starbucks-confluent-scope --key kafka-bootstrap-servers --string-value "<KAFKA_BOOTSTRAP_URL>"
databricks secrets put-secret --scope starbucks-confluent-scope --key kafka-api-key --string-value "<CONFLUENT_API_KEY>"
databricks secrets put-secret --scope starbucks-confluent-scope --key kafka-api-secret --string-value "<CONFLUENT_SECRET>"
databricks secrets put-secret --scope starbucks-confluent-scope --key schema-registry-url --string-value "<SCHEMA_REGISTRY_URL>"
databricks secrets put-secret --scope starbucks-confluent-scope --key schema-registry-api-key --string-value "<SR_API_KEY>"
databricks secrets put-secret --scope starbucks-confluent-scope --key schema-registry-api-secret --string-value "<SR_SECRET>"
```

### Step 2: Deploy and Run via Databricks Workflows
```bash
# Upload artifacts to DBFS or Databricks Workspace
databricks fs cp src/starbucks_store_operations_pipeline.py dbfs:/FileStore/starbucks/src/starbucks_store_operations_pipeline.py
databricks fs cp config/pipeline_config.json dbfs:/FileStore/starbucks/config/pipeline_config.json

# Deploy workflow job
databricks jobs create --json-file databricks/databricks_job.json
```

### Step 3: Local Verification & Testing
```bash
# Run salting unit test
python -m unittest tests/test_salting_logic.py

# Run mock telemetry stream simulation
python src/producer_simulator.py
```
