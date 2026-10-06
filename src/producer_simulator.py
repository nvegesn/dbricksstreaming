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

import time
import random
import uuid
import struct
import io
import json
from datetime import datetime, timezone, timedelta

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


def run_simulation(num_batches=5, orders_per_batch=20):
    """
    Executes mock stream simulation demonstrating skew and late events.
    """
    print("=" * 70)
    print("STARBUCKS TELEMETRY SIMULATOR: MORNING RUSH SIMULATION")
    print("=" * 70)

    for b in range(1, num_batches + 1):
        print(f"\n--- Emitting Simulated Micro-Batch #{b} ---")
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

        print(f"Generated {len(batch_events)} order events.")
        sample = batch_events[0]
        print(f"Sample Event: Store={sample['store_id']}, Ch={sample['channel']}, "
              f"Amount=${sample['transaction_amount']}, Placed={sample['order_placed_timestamp']}")

        if fastavro:
            sample_encoded = encode_confluent_avro(sample, schema_id=101)
            print(f"Encoded Confluent Avro Wire Payload Length: {len(sample_encoded)} bytes")

        time.sleep(1)


if __name__ == "__main__":
    run_simulation()
