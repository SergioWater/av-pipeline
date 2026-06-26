"""
Stage 3a — Step 1: the simplest possible producer.

Goal: prove Python can connect to Redpanda and send ONE structured
telemetry message into the 'raw-vehicle-telemetry' topic.

Run it (with your 3.11 venv activated):
    python producer/send_one.py
"""

import json
from datetime import datetime, timezone
from confluent_kafka import Producer

# ---------------------------------------------------------------------
# 1. Tell the producer WHERE the broker is.
#    localhost:19092 is the "external" port from your docker-compose.yml
#    (the one your machine uses to reach the Redpanda container).
# ---------------------------------------------------------------------
config = {"bootstrap.servers": "localhost:19092"}
producer = Producer(config)

TOPIC = "raw-vehicle-telemetry"


# ---------------------------------------------------------------------
# 2. A callback: Redpanda calls this back to tell us if the message
#    was delivered successfully or failed. This is how we SEE it work.
# ---------------------------------------------------------------------
def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery FAILED: {err}")
    else:
        print(
            f"✅ Delivered to topic '{msg.topic()}' "
            f"partition {msg.partition()} offset {msg.offset()}"
        )


# ---------------------------------------------------------------------
# 3. Build ONE fake telemetry message (a Python dict -> JSON string).
# ---------------------------------------------------------------------
message = {
    "vehicle_id": "AV-0427",
    "timestamp": datetime.now(timezone.utc).isoformat(),
    "latitude": 37.3361,
    "longitude": -121.8906,
    "speed_mph": 24.5,
    "battery_temp_c": 41.2,
    "sensor_status": {"lidar": "OK", "camera": "OK", "radar": "DEGRADED"},
}

# Convert the dict into a JSON string, then into bytes (brokers store bytes).
payload = json.dumps(message).encode("utf-8")

# ---------------------------------------------------------------------
# 4. Send it. We use vehicle_id as the KEY so all messages from the same
#    vehicle land in the same partition (consistent routing).
# ---------------------------------------------------------------------
print(f"Sending one message to '{TOPIC}'...")
producer.produce(
    TOPIC,
    key=message["vehicle_id"].encode("utf-8"),
    value=payload,
    callback=delivery_report,
)

# ---------------------------------------------------------------------
# 5. flush() forces the producer to actually send and wait for the
#    delivery report before the program exits. Without this, the script
#    might quit before the message leaves the buffer.
# ---------------------------------------------------------------------
producer.flush()
print("Done.")
