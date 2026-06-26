# Stage 2 — Message Broker (Redpanda) ✅

## What this stage is
The **broker** is a buffer/queue that sits between the data **producer** (simulator)
and the data **consumer** (Spark). The producer drops messages in; the consumer
picks them up later. The broker holds messages safely in between.

```
[Simulator]  --drops messages-->  [REDPANDA broker]  --read later-->  [Spark]
   fast                            holds / queues                      slower
                                   (THIS STAGE)
```

## Key concepts
| Term | Meaning |
|---|---|
| **Broker** | The queue program. We use **Redpanda** (a lighter, Java-free Kafka). |
| **Kafka vs Redpanda** | Same kind of thing. Redpanda speaks Kafka's language; we run only Redpanda. |
| **Topic** | A named channel inside the broker. Ours: `raw-vehicle-telemetry`. |
| **Partition** | A parallel "lane" inside a topic. We have 3 (0, 1, 2). Messages spread across them. |
| **Replica** | A backup copy of data on another broker. We have 1 (no redundancy — fine locally). |
| **Offset** | A message's position number within its partition (starts at 0). |
| **High-watermark** | Count of messages in a partition. |

## Why a broker at all? (instead of producer → Spark directly)
- **Speed mismatch:** producer is fast, Spark consumes in slower batches. Broker absorbs bursts.
- **Decoupling:** if Spark crashes, messages wait safely in the broker; nothing is lost.
- **Replay:** you can re-read old messages.

## Commands cheat-sheet (run from PowerShell)

```powershell
# Is the broker running?
docker ps                                   # look for container "redpanda" = Up

# List topics (shows partition + replica counts)
docker exec -it redpanda rpk topic list

# Create the topic with 3 partitions
docker exec -it redpanda rpk topic create raw-vehicle-telemetry -p 3

# Inspect partitions + message counts (HIGH-WATERMARK = messages per partition)
docker exec -it redpanda rpk topic describe raw-vehicle-telemetry -p

# Send a test message (note -i, not -it, when piping)
"hello broker test 1" | docker exec -i redpanda rpk topic produce raw-vehicle-telemetry

# Read messages from the very beginning (use a larger --num so it sweeps ALL partitions)
docker exec -it redpanda rpk topic consume raw-vehicle-telemetry --num 5 --offset start
```

## Gotchas learned
- **`--num 1` can miss your message** — messages are spread across 3 partitions, so a
  small limit may read empty partitions and stop. Use `--num 5` (or more) to sweep all lanes.
- **Console UI shows nothing by default** — it defaults to "Newest" (future messages only).
  Switch to **Oldest** and refresh: Console → Topics → raw-vehicle-telemetry → Messages.
- **`\r` on message values** is just a Windows carriage return from PowerShell piping. Harmless.
- **`docker compose down` wipes the broker** (no Redpanda volume defined) — use `stop`/`start`
  to keep data. MinIO has a volume so its data persists either way.

## Done when
- [x] `redpanda` container Up
- [x] Console reachable at http://localhost:8080
- [x] Topic `raw-vehicle-telemetry` exists with 3 partitions
- [x] Produced a message and consumed it back (saw partition + offset)

## Next: Stage 3 prep — the Simulator
The broker is empty plumbing. Next we build the **Vehicle Telemetry Simulator**:
a Python script that auto-generates realistic vehicle JSON and streams it into
`raw-vehicle-telemetry` continuously (with ~1% deliberate bad data for later stages).
