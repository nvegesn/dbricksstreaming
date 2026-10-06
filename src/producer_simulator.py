"""
================================================================================
SCRIPT: producer_simulator.py
ROLE: Integration Testing & Verification Harness

PURPOSE:
  Simulates live Starbucks order telemetry emitted to Confluent Kafka to test:
  1. Morning rush burst traffic (backpressure test).
  2. Times Square data skew (100x traffic on STORE-NYC-7381).
  3. Subway commuter late-arriving events (12 minutes behind clock).
  4. Dynamic Avro serialization with Confluent wire format (magic byte + schema ID).
================================================================================
"""

import os
import sys
import argparse
import time
import random
import uuid
import struct
import io
import json
from datetime import datetime, timezone, timedelta

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    import requests
except ImportError:
    requests = None

try:
    import fastavro
except ImportError:
    fastavro = None

try:
    from confluent_kafka import Producer
except ImportError:
    Producer = None

AVRO_SCHEMA_DEF = {
    "type": "record",
    "name": "LiveOrderEvent",
    "namespace": "com.starbucks.analytics.orders",
    "fields": [
        {"name": "order_id", "type": "string"},
        {"name": "store_id", "type": "string"},
        {"name": "customer_id", "type": ["null", "string"], "default": None},
        {"name": "channel", "type": "string"},
        {"name": "order_placed_timestamp", "type": "string"},
        {"name": "order_fulfilled_timestamp", "type": ["null", "string"], "default": None},
        {"name": "transaction_amount", "type": "double"},
        {
            "name": "items",
            "type": {
                "type": "array",
                "items": {
                    "type": "record",
                    "name": "OrderItem",
                    "fields": [
                        {"name": "item_id", "type": "string"},
                        {"name": "item_name", "type": "string"},
                        {"name": "quantity", "type": "int"},
                        {"name": "unit_price", "type": "double"},
                        {
                            "name": "seasonal_syrup_modifiers",
                            "type": {
                                "type": "array",
                                "items": {
                                    "type": "record",
                                    "name": "SyrupModifier",
                                    "fields": [
                                        {"name": "syrup_name", "type": "string"},
                                        {"name": "pumps", "type": "int"}
                                    ]
                                }
                            },
                            "default": []
                        }
                    ]
                }
            },
            "default": []
        },
        {
            "name": "payment",
            "type": {
                "type": "record",
                "name": "PaymentDetails",
                "fields": [
                    {"name": "primary_tender", "type": "string"},
                    {
                        "name": "split_tender",
                        "type": {
                            "type": "array",
                            "items": {
                                "type": "record",
                                "name": "SplitTenderDetail",
                                "fields": [
                                    {"name": "tender_type", "type": "string"},
                                    {"name": "amount", "type": "double"}
                                ]
                            }
                        },
                        "default": []
                    }
                ]
            }
        }
    ]
}

STORES = [
    "STORE-NYC-7381",     # TIMES SQUARE FLAGSHIP (Heavy Skew!)
    "STORE-CHI-9921",     # CHICAGO RESERVE ROASTERY
    "STORE-SEA-0101",     # PIKE PLACE ORIGINAL
    "STORE-AUSTIN-110",   # Suburban Drive-Thru
    "STORE-DENVER-404",   # Suburban Walk-In
    "STORE-ORL-782",      # Highway Travel Plaza
]

ITEMS = [
    ("BEV-PSL-001", "Pumpkin Spice Latte", 6.25, [("Pumpkin Spice", 3)]),
    ("BEV-CB-002", "Vanilla Sweet Cream Cold Brew", 5.45, [("Vanilla", 2)]),
    ("BEV-CM-003", "Caramel Macchiato", 5.95, [("Vanilla", 3), ("Caramel Drizzle", 1)]),
    ("BEV-BSE-004", "Brown Sugar Oatmilk Shaken Espresso", 6.45, [("Brown Sugar", 4)]),
    ("FOOD-CROIS-01", "Butter Croissant", 3.95, []),
]


def generate_simulated_order(is_times_square=False, is_subway_commuter=False):
    """
    Generates a realistic Starbucks order event demonstrating business edge cases.
    """
    now_utc = datetime.now(timezone.utc)
    
    if is_subway_commuter:
        # Subway commuter: Order placed 12 minutes ago while entering underground tunnel
        placed_dt = now_utc - timedelta(minutes=12, seconds=random.randint(10, 45))
        channel = "MOBILE_APP_PICKUP"
        store_id = "STORE-NYC-7381"
    else:
        placed_dt = now_utc - timedelta(seconds=random.randint(60, 300))
        if is_times_square:
            store_id = "STORE-NYC-7381"
            channel = random.choice(["IN_STORE_POS", "MOBILE_APP_PICKUP"])
        else:
            store_id = random.choice(STORES)
            channel = random.choice(["IN_STORE_POS", "DRIVE_THRU", "MOBILE_APP_PICKUP"])

    # Wait time simulation (barista prep time: 90s to 450s)
    fulfilled_dt = placed_dt + timedelta(seconds=random.randint(90, 450))

    # Pick 1-3 items
    selected_items = random.sample(ITEMS, k=random.randint(1, 3))
    order_items = []
    total_amount = 0.0

    for item_id, name, price, syrups in selected_items:
        qty = random.randint(1, 2)
        total_amount += price * qty
        modifiers = [{"syrup_name": s[0], "pumps": s[1]} for s in syrups]
        order_items.append({
            "item_id": item_id,
            "item_name": name,
            "quantity": qty,
            "unit_price": price,
            "seasonal_syrup_modifiers": modifiers
        })

    total_amount = round(total_amount, 2)

    # Split tender edge case (e.g. Starbucks Card + Apple Pay)
    if random.random() < 0.3:
        sbux_card_amount = round(total_amount * 0.6, 2)
        rem_amount = round(total_amount - sbux_card_amount, 2)
        payment = {
            "primary_tender": "STARBUCKS_CARD",
            "split_tender": [
                {"tender_type": "STARBUCKS_CARD", "amount": sbux_card_amount},
                {"tender_type": "APPLE_PAY", "amount": rem_amount}
            ]
        }
    else:
        payment = {
            "primary_tender": "STARBUCKS_CARD",
            "split_tender": []
        }

    return {
        "order_id": f"ORD-{uuid.uuid4().hex[:12].upper()}",
        "store_id": store_id,
        "customer_id": f"MSR-{random.randint(100000, 999999)}",
        "channel": channel,
        "order_placed_timestamp": placed_dt.isoformat(),
        "order_fulfilled_timestamp": fulfilled_dt.isoformat(),
        "transaction_amount": total_amount,
        "items": order_items,
        "payment": payment
    }


def encode_confluent_avro(record, schema_id=101):
    """
    Serializes record into Confluent wire format:
    [Byte 0: Magic Byte 0x00] + [Bytes 1-4: Big-Endian Schema ID] + [Raw Avro Bytes]
    """
    if fastavro is None:
        raise RuntimeError("fastavro library is required for Avro serialization.")

    parsed_schema = fastavro.parse_schema(AVRO_SCHEMA_DEF)
    buffer = io.BytesIO()
    # Write Confluent 5-byte header
    buffer.write(b'\x00')
    buffer.write(struct.pack('>I', schema_id))
    # Write Avro payload
    fastavro.schemaless_writer(buffer, parsed_schema, record)
    return buffer.getvalue()


def fetch_schema_id_from_registry(sr_url, sr_api_key, sr_api_secret, subject="starbucks_live_orders-value"):
    """
    Attempts to fetch the registered schema ID dynamically from Confluent Schema Registry.
    """
    if not sr_url or not requests:
        return None
    try:
        url = f"{sr_url.rstrip('/')}/subjects/{subject}/versions/latest"
        auth = (sr_api_key, sr_api_secret) if sr_api_key and sr_api_secret else None
        headers = {"Accept": "application/vnd.schemaregistry.v1+json"}
        resp = requests.get(url, auth=auth, headers=headers, timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            schema_id = data.get("id")
            print(f"[SCHEMA REGISTRY] Dynamically retrieved Schema ID {schema_id} for subject '{subject}'")
            return schema_id
        else:
            print(f"[SCHEMA REGISTRY WARNING] Could not fetch schema ID (HTTP {resp.status_code}): {resp.text}")
    except Exception as e:
        print(f"[SCHEMA REGISTRY WARNING] Exception fetching schema ID: {e}")
    return None


def run_simulation(
    num_batches=5,
    orders_per_batch=20,
    interval_seconds=1.0,
    is_live=False,
    continuous=False,
    bootstrap_servers=None,
    api_key=None,
    api_secret=None,
    sr_url=None,
    sr_api_key=None,
    sr_api_secret=None,
    topic="starbucks_live_orders",
    schema_id=None
):
    """
    Executes mock stream simulation demonstrating skew and late events.
    Optionally streams directly to Confluent Cloud Kafka if is_live=True.
    """
    print("=" * 70)
    print("STARBUCKS TELEMETRY SIMULATOR: MORNING RUSH SIMULATION")
    print(f"Mode: {'LIVE CONFLUENT KAFKA PRODUCER' if is_live else 'MOCK SIMULATION (DRY RUN)'}")
    print("=" * 70)

    producer = None
    if is_live:
        if Producer is None:
            raise RuntimeError("confluent-kafka Python library is not installed. Run: pip install confluent-kafka")
        if not bootstrap_servers or not api_key or not api_secret:
            raise ValueError("Live mode requires bootstrap_servers, api_key, and api_secret!")

        producer_conf = {
            "bootstrap.servers": bootstrap_servers,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": api_key,
            "sasl.password": api_secret,
            "client.id": "starbucks-simulator-producer",
            "linger.ms": 10,
            "acks": "all"
        }
        producer = Producer(producer_conf)
        print(f"[KAFKA] Connected producer to {bootstrap_servers} (Topic: {topic})")

        # Resolve Schema ID
        if schema_id is None and sr_url:
            schema_id = fetch_schema_id_from_registry(
                sr_url, sr_api_key, sr_api_secret, subject=f"{topic}-value"
            )
        if schema_id is None:
            schema_id = 101
            print(f"[SCHEMA REGISTRY] Using default Schema ID: {schema_id}")

    delivered_count = 0
    failed_count = 0

    def delivery_report(err, msg):
        nonlocal delivered_count, failed_count
        if err is not None:
            failed_count += 1
            print(f"[PRODUCE ERROR] Delivery failed for message {msg.key()}: {err}")
        else:
            delivered_count += 1

    batch_idx = 0
    try:
        while True:
            batch_idx += 1
            if not continuous and batch_idx > num_batches:
                break

            print(f"\n--- Emitting Batch #{batch_idx} ({orders_per_batch} orders) ---")
            batch_events = []

            # 1. Flagship Store Skew (Times Square): 65% of volume
            for _ in range(int(orders_per_batch * 0.65)):
                batch_events.append(generate_simulated_order(is_times_square=True))

            # 2. Subway Commuter (12-minute late arrival): 10% of volume
            for _ in range(max(1, int(orders_per_batch * 0.10))):
                batch_events.append(generate_simulated_order(is_subway_commuter=True))

            # 3. Regular Stores: 25% of volume
            for _ in range(int(orders_per_batch * 0.25)):
                batch_events.append(generate_simulated_order(is_times_square=False))

            sample = batch_events[0]
            print(f"Sample Event: Store={sample['store_id']}, Ch={sample['channel']}, "
                  f"Amount=${sample['transaction_amount']}, Placed={sample['order_placed_timestamp']}")

            if is_live and producer:
                active_schema_id = int(schema_id or 101)
                for order in batch_events:
                    payload = encode_confluent_avro(order, schema_id=active_schema_id)
                    producer.produce(
                        topic=topic,
                        key=order["store_id"].encode("utf-8"),
                        value=payload,
                        on_delivery=delivery_report
                    )
                producer.poll(0)
                print(f"[KAFKA] Dispatched {len(batch_events)} orders to topic '{topic}'.")
            else:
                if fastavro:
                    sample_encoded = encode_confluent_avro(sample, schema_id=101)
                    print(f"Encoded Confluent Avro Wire Payload Length: {len(sample_encoded)} bytes")

            time.sleep(interval_seconds)

    except KeyboardInterrupt:
        print("\n[STOP] Received KeyboardInterrupt. Shutting down generator...")

    if is_live and producer:
        print("[KAFKA] Flushing producer buffer...")
        producer.flush(10)
        print(f"[KAFKA] Final Delivery Stats: {delivered_count} delivered successfully, {failed_count} failed.")

    print(f"\n[DONE] Simulation completed. Processed {batch_idx if continuous else min(batch_idx, num_batches)} batches.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Starbucks Telemetry Order Stream Simulator")
    parser.add_argument("--live", action="store_true", help="Publish directly to live Confluent Cloud Kafka")
    parser.add_argument("--continuous", action="store_true", help="Stream indefinitely until stopped (Ctrl+C)")
    parser.add_argument("--batches", type=int, default=5, help="Number of batches to emit (default: 5)")
    parser.add_argument("--orders-per-batch", type=int, default=20, help="Orders per batch (default: 20)")
    parser.add_argument("--interval", type=float, default=1.0, help="Seconds between batches (default: 1.0)")
    parser.add_argument("--topic", type=str, default=os.getenv("KAFKA_TOPIC", "starbucks_live_orders"))
    parser.add_argument("--schema-id", type=int, default=int(os.getenv("SCHEMA_ID")) if os.getenv("SCHEMA_ID") else None)
    parser.add_argument("--bootstrap-servers", type=str, default=os.getenv("KAFKA_BOOTSTRAP_SERVERS"))
    parser.add_argument("--api-key", type=str, default=os.getenv("KAFKA_API_KEY"))
    parser.add_argument("--api-secret", type=str, default=os.getenv("KAFKA_API_SECRET"))
    parser.add_argument("--schema-registry-url", type=str, default=os.getenv("SCHEMA_REGISTRY_URL"))
    parser.add_argument("--sr-api-key", type=str, default=os.getenv("SCHEMA_REGISTRY_API_KEY"))
    parser.add_argument("--sr-api-secret", type=str, default=os.getenv("SCHEMA_REGISTRY_API_SECRET"))

    args = parser.parse_args()

    # If live flag is explicitly set OR both bootstrap servers and api key are available and live flag wasn't explicitly denied
    is_live_mode = args.live or (args.bootstrap_servers and args.api_key and args.api_secret and "--no-live" not in sys.argv)

    run_simulation(
        num_batches=args.batches,
        orders_per_batch=args.orders_per_batch,
        interval_seconds=args.interval,
        is_live=is_live_mode,
        continuous=args.continuous,
        bootstrap_servers=args.bootstrap_servers,
        api_key=args.api_key,
        api_secret=args.api_secret,
        sr_url=args.schema_registry_url,
        sr_api_key=args.sr_api_key,
        sr_api_secret=args.sr_api_secret,
        topic=args.topic,
        schema_id=args.schema_id
    )
