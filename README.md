# Autonomous Vehicle Telemetry Pipeline

Real-time vehicle telemetry pipeline built with Python, Kafka/Redpanda, Docker, and MinIO.

## What It Does

- Generates simulated autonomous-vehicle sensor data with Python.
- Streams vehicle data through a Kafka-compatible message broker.
- Runs the main services together with Docker Compose.
- Stores telemetry in S3-compatible object storage for later analysis.

## Tech

Python, Redpanda/Kafka, Docker Compose, MinIO, Spark Structured Streaming, Delta Lake

## How It Works

```text
Vehicle data producer -> Redpanda/Kafka -> consumer/processing -> MinIO storage
```

The project separates data generation, streaming, processing, and storage so each part can be developed and tested independently.

## Running Locally

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
docker compose up -d
```

## Engineering Notes

The local environment uses isolated services and health checks so the pipeline can be started consistently without depending on external infrastructure.
