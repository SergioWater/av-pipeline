"""
Stage 3 — Step 1: the simplest possible CONSUMER.

GOAL OF THIS FILE:
    Prove that Python can READ the messages your simulator is putting into
    Redpanda. This is the mirror image of send_one.py — instead of sending,
    it receives. It just connects and PRINTS each message. No parsing, no
    rules yet. That comes in later steps.

This is the FIRST half of the "engine" (the consumer). Spark will replace
this later and do the same thing but in parallel + at scale.

HOW TO RUN (two terminals, both with the .venv activated):
    Terminal 1:  python simulator.py        # keeps producing data
    Terminal 2:  python consumer/read_stream.py   # reads & prints it
"""

from confluent_kafka import Consumer
import json
# ---------------------------------------------------------------------
# 1. Configure the consumer.
#    - bootstrap.servers: WHERE the broker is (same as the producer).
#    - group.id: a name for THIS reader. Kafka/Redpanda remembers how far
#      a group has read, so it doesn't re-read old messages. Any name works.
#    - auto.offset.reset = "earliest": if this group has never read before,
#      start from the OLDEST message. (Use "latest" to only get new ones.)
# ---------------------------------------------------------------------
config = {
    "bootstrap.servers": "localhost:19092",
    "group.id": "telemetry-reader",
    "auto.offset.reset": "earliest",
}

consumer = Consumer(config)
TOPIC = "raw-vehicle-telemetry"

# ---------------------------------------------------------------------
# 2. Subscribe: tell the consumer which topic(s) to listen to.
# ---------------------------------------------------------------------
consumer.subscribe([TOPIC])
print(f"👂 Listening to '{TOPIC}'... (press Ctrl+C to stop)\n")

# ---------------------------------------------------------------------
# 3. The read loop. poll() asks the broker "any new messages?"
#    - It waits up to 1.0 second for a message, then returns None if none.
#    - If we get a message, we print its raw value.
# ---------------------------------------------------------------------
try:
    while True:
        msg = consumer.poll(1.0)  # wait up to 1s for a message

        if msg is None:
            continue  # no message this round, ask again

        if msg.error():
            print(f"❌ Consumer error: {msg.error()}")
            continue

        # msg.value() is raw bytes -> decode back to a text string.
        raw_text = msg.value().decode("utf-8")
        key = msg.key().decode("utf-8") if msg.key() else "(no key)"

        data = json.loads(raw_text)
        
        vid     = data["vehicle_id"]
        speed   = data["speed_mph"]
        battery = data["battery_temp_c"]
        sensors = data["sensor_status"]          # a nested dict
        lidar   = sensors["lidar"]
        camera  = sensors["camera"]
        radar   = sensors["radar"]
  
        # --- RULE 3.3: decide if this message is a problem ---
        sensor_failed = "FAILED" in (lidar, camera, radar)
        too_hot = battery > 45

        # Build a flag string: 🚨 if there's a problem, blank if fine.
        if sensor_failed or too_hot:
            flag = "🚨 ALERT"
        else:
            flag = "      OK"

        # --- One clean line per message, now with the flag ---
        print(f"{flag} | {vid} | speed {speed:>5} mph | battery {battery:>4}°C "
              f"| lidar {lidar}, camera {camera}, radar {radar}")
finally:
    # close() leaves the group cleanly so offsets are committed.
    consumer.close()
    print("Consumer closed.")
