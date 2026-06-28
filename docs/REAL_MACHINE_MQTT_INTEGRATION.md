# Real Machine MQTT Integration

The project now supports two ingestion entry points:

```text
1) Direct Kafka simulation:
   kafka_producer -> Kafka -> raw consumer -> PostgreSQL

2) Machine-protocol path:
   real machine or mqtt_simulator -> Mosquitto MQTT broker -> mqtt_bridge -> Kafka -> raw consumer -> PostgreSQL
```

## What is guaranteed by the code

The repository guarantees a working internal protocol path when the Docker stack is running:

```text
mqtt-simulator -> mosquitto -> mqtt-bridge -> kafka -> consumer -> PostgreSQL raw_sensor_data
```

The bridge validates that every MQTT payload is a JSON object and contains at least:

```text
engine_id
time_in_cycles
```

It then normalises the reading to the same sensor schema used by the Kafka consumer and forwards it to Kafka with an idempotency key:

```text
source_file:engine_id:time_in_cycles
```

The PostgreSQL consumer still protects the raw table with a uniqueness constraint on:

```text
source_file, engine_id, time_in_cycles
```

So repeated messages do not create duplicate raw rows.

## What cannot be guaranteed without factory details

Actual physical-machine communication needs the real machine/broker details:

```text
broker host/IP
port
username/password or certificates, if enabled
topic names
payload format
QoS requirement
network/firewall access
machine heartbeat/status rules
```

Without these details, the repository can guarantee the protocol layer and simulation, not a live factory connection.

## Expected MQTT topic

Default topic:

```text
sensors/cmapss
```

Change it through:

```text
MQTT_TOPIC
```

## Minimal test payload

```json
{
  "source_file": "REAL_MACHINE_01",
  "engine_id": 1,
  "time_in_cycles": 1,
  "s2": 641.82,
  "s3": 1589.70,
  "s4": 1400.60,
  "s7": 554.36,
  "s11": 47.47,
  "s12": 521.66,
  "s15": 8.4195,
  "s20": 39.06,
  "s21": 23.419
}
```

## Quick protocol test

Start the stack:

```powershell
docker compose up -d --build
```

Publish from the simulator:

```powershell
docker compose run --rm mqtt-simulator FD001 --start-row 1 --limit 10 --sleep 0
```

Check storage:

```powershell
docker compose exec postgres psql -U anomx -d anomx_db -c "SELECT source_file, COUNT(*) FROM raw_sensor_data GROUP BY source_file ORDER BY source_file;"
```

Send one custom machine reading through the helper:

```powershell
docker compose run --rm machine-mqtt-test
```

Then check it:

```powershell
docker compose exec postgres psql -U anomx -d anomx_db -c "SELECT source_file, engine_id, time_in_cycles FROM raw_sensor_data WHERE source_file='REAL_MACHINE_TEST' ORDER BY id DESC LIMIT 5;"
```
