import asyncio
import json
import random
import time
from datetime import datetime, timezone
from confluent_kafka import Producer

# 1. Configure the Redpanda Producer
config = {"bootstrap.servers": "localhost:19092"}
producer = Producer(config)
TOPIC = "raw-vehicle-telemetry"

def delivery_report(err, msg):
    """Callback to verify message delivery to Redpanda."""
    if err is not None:
        print(f"❌ Delivery FAILED: {err}")
    # else:
    #     print(f"✅ Delivered to partition {msg.partition()}")

async def simulate_vehicle(vehicle_id, start_lat=37.3361, start_lon=-121.8906):
    """Generates and produces continuous telemetry for a single AV."""
    current_lat = start_lat
    current_lon = start_lon
    sensor_states = ["OK", "OK", "OK", "DEGRADED", "FAILED"]

    for _ in range(60):
        # Simulate slight movement
        current_lat += random.uniform(-0.0001, 0.0001)
        current_lon += random.uniform(-0.0001, 0.0001)

        # Build the payload
        message = {
            "vehicle_id": vehicle_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "latitude": round(current_lat, 6),
            "longitude": round(current_lon, 6),
            "speed_mph": round(random.uniform(0, 65), 1),
            "battery_temp_c": round(random.uniform(20, 50), 1),
            "sensor_status": {
                "lidar": random.choice(sensor_states),
                "camera": random.choice(sensor_states),
                "radar": random.choice(sensor_states)
            }
        }
        
        payload = json.dumps(message).encode("utf-8")
        key = message["vehicle_id"].encode("utf-8")

        # Produce to Redpanda
        producer.produce(
            TOPIC,
            key=key,
            value=payload,
            callback=delivery_report
        )
        
        # Trigger any available delivery callbacks
        producer.poll(0)

        # Sleep to simulate the tick-rate
        await asyncio.sleep(1) 

async def main():
    print(f"Starting simulation. Blasting data to '{TOPIC}'...")
    
    # Start with 5 vehicles
    tasks = [
        simulate_vehicle(f"AV-042{i}", 37.3361 + (i * 0.001), -121.8906) 
        for i in range(5)
    ]
    
    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        print("\nStopping simulation...")
    finally:
        print("Flushing final messages to broker...")
        producer.flush()

if __name__ == "__main__":
    asyncio.run(main())