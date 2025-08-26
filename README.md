# clickhouse-loadtest

Load-testing utilities for **near-real-time ingestion** and **freshness** in ClickHouse.

Supports multiple ingestion paths:

- **Direct HTTP mode** (Node writer → ClickHouse)
- **Kafka → Node sink → ClickHouse**
- **Kafka → ClickHouse Kafka engine + Materialized View (MV)**

Also included:

- **Reader**: monitors freshness, rows/sec, parts
- **Kafka installer (KRaft)** + topic partition helper
- **Pause/resume scripts** for Node sink maintenance

---

## Directory layout

```
.
├─ writer/                              # direct HTTP writer to ClickHouse
├─ reader/                              # observability (lag, parts, totals)
├─ writer_kafka_node_consumer/          # Kafka producer + Node sink → ClickHouse
├─ writer_kafka_clickhouse_consumer/    # Kafka producer + ClickHouse Kafka engine + MV
└─ scripts/                             # Kafka installer + helpers
```

---

## 1) Install Kafka (single node, KRaft)

```bash
cd scripts
sudo ./install.sh   TOPIC_NAME=clickhouse_ingest   PARTITIONS=1   ADVERTISED_HOST=localhost
```

- Runs under `systemd` as user `kafka`
- Logs: `journalctl -u kafka -f`
- Default broker: `localhost:9092`

**Increase partitions later:**

```bash
./set_partitions.sh clickhouse_ingest 3
```

---

## 2) Direct HTTP mode

### Writer

```bash
cd writer
cp .env.example .env
npm i
npm start
```

`.env.example`
```env
CH_HOST=http://localhost:8123
CH_DATABASE=default
CH_USER=default
CH_PASSWORD=12345678.

TABLE_NAME=drill_events_test

INSERT_RATE=10000
BATCH_SIZE=400000
FLUSH_MS=30000

ASYNC_INSERT=1
WAIT_FOR_ASYNC_INSERT=1
# ASYNC_INSERT_BUSY_TIMEOUT_MS=40000
# MIN_INSERT_BLOCK_SIZE_ROWS=100000
# MIN_INSERT_BLOCK_SIZE_BYTES=33554432
```

### Reader

```bash
cd reader
cp .env.example .env
npm i
npm start
```

`.env.example`
```env
CH_HOST=http://localhost:8123
CH_DATABASE=default
CH_USER=default
CH_PASSWORD=12345678.
TABLE_NAME=drill_events_test

QUERIES_PER_MIN=30
QUERY_WINDOW_SEC=60
```

---

## 3) Kafka → Node sink → ClickHouse

Kafka buffers the firehose; Node sink accumulates (e.g. **20s worth**) then inserts to ClickHouse.

### Producer

```bash
cd writer_kafka_node_consumer
cp .env.example .env
npm i
node producer.js
```

### Sink (consumer → ClickHouse)

```bash
node consumer_ch_sink.js
```

`.env.example`
```env
# Kafka
KAFKA_BROKERS=localhost:9092
KAFKA_TOPIC=clickhouse_ingest
KAFKA_CREATE_TOPIC=1
KAFKA_CLIENT_ID=writer-kafka
KAFKA_BATCH_SIZE=5000
KAFKA_FLUSH_MS=200
INSERT_RATE=10000
PRODUCER_ACKS=all
PRODUCER_LINGER_MS=5
PRODUCER_COMPRESSION=gzip   # fallback from zstd

# Sink → ClickHouse
CH_HOST=http://localhost:8123
CH_DATABASE=default
CH_USER=default
CH_PASSWORD=12345678.
TABLE_NAME=drill_events_test

BUFFER_SECONDS=20
MAX_BUFFER_ROWS=1000000

ASYNC_INSERT=1
WAIT_FOR_ASYNC_INSERT=1
```

**At-least-once semantics**  
Offsets are committed **only after successful ClickHouse insert**.

**Pause/resume (maintenance):**
```bash
node pause_consumer.js  --brokers localhost:9092 --group writer-kafka-sink
node resume_consumer.js --brokers localhost:9092 --group writer-kafka-sink
```

---

## 4) Kafka → ClickHouse Kafka engine + MV

ClickHouse consumes directly from Kafka into a MergeTree table.

### Setup

```bash
cd writer_kafka_clickhouse_consumer
cp .env.example .env
npm i
npm run setup   # runs setup_ch_kafka.js
```

`.env.example`
```env
# Kafka → CH Kafka engine
KAFKA_BROKERS=localhost:9092
KAFKA_TOPIC=clickhouse_ingest
KAFKA_GROUP=ch_group

# ClickHouse
CH_HOST=http://localhost:8123
CH_DATABASE=default
CH_USER=default
CH_PASSWORD=12345678.

TARGET_TABLE=drill_events_test
KAFKA_SRC_TABLE=kafka_src
MV_NAME=mv_kafka_to_events

STREAM_FLUSH_INTERVAL_MS=20000
KAFKA_MAX_BLOCK_SIZE=100000
KAFKA_POLL_MAX_BATCH_SIZE=100000
KAFKA_POLL_TIMEOUT_MS=1000
```

Run a producer:
```bash
node producer.js
```

For parallelism, increase **partitions** and set `kafka_num_consumers=N`.

---

## Observability SQL

**Recent inserts**
```sql
SELECT event_time, written_rows
FROM system.query_log
WHERE type='QueryFinish' AND query_kind='Insert'
  AND tables = 'drill_events_test'
ORDER BY event_time DESC
LIMIT 10;
```

**Async insert coalescing**
```sql
SELECT event_time, rows
FROM system.asynchronous_insert_log
WHERE table='drill_events_test'
ORDER BY event_time DESC
LIMIT 10;
```

**Lag**
```sql
WITH toUnixTimestamp64Milli(now64()) AS now_ms
SELECT now_ms - max(ts) AS lag_ms
FROM drill_events_test;
```

**Parts**
```sql
SELECT countIf(active) AS active_parts
FROM system.parts
WHERE table='drill_events_test';
```

---

## Tuning

- **Topic partitions**: `set_partitions.sh clickhouse_ingest 3`
- **Producer**: `KAFKA_BATCH_SIZE`, `PRODUCER_COMPRESSION=gzip|none`
- **Sink**: `BUFFER_SECONDS=10–30`, `MAX_BUFFER_ROWS`
- **ClickHouse**: tune `min_insert_block_size_rows` / `bytes` under heavy load

---

## Maintenance

- **Pause/resume** Node sink group → backlog stored in Kafka
- **Rolling restart**: stop sink, Kafka buffers, restart sink, backlog drains
- **Alter partitions**: `./scripts/set_partitions.sh <topic> <n>`

---

## Troubleshooting

- `ZSTD compression not implemented` → use `PRODUCER_COMPRESSION=gzip` or `none`
- Sink OOM → reduce `BUFFER_SECONDS`, raise `MAX_BUFFER_ROWS`, run multiple consumers
- Part explosion → avoid 1-row inserts, prefer batching
- Consumer lag grows → add partitions, consumers, or shorten buffer time

---

## License

MIT
