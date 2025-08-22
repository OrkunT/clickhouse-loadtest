# clickhouse-loadtest

Two small Node.js apps for load testing ClickHouse near‑real‑time ingestion & freshness.

- **writer** inserts synthetic events at a configurable rate with batching + async inserts.
- **reader** periodically measures freshness (ms lag), rolling counts, and active part totals.

## Quick start

```bash
# writer
cd writer
cp .env.example .env     # adjust INSERT_RATE/BATCH_SIZE/FLUSH_MS if desired
npm i
npm start

# reader (separate terminal)
cd reader
cp .env.example .env     # adjust QUERIES_PER_MIN / QUERY_WINDOW_SEC
npm i
npm start
```

## Table schema

Writer creates a daily‑partitioned MergeTree table named `drill_events` with JSON columns:

```sql
CREATE TABLE IF NOT EXISTS drill_events
(
  a   LowCardinality(String),
  e   LowCardinality(String),
  uid String,
  did String,
  lsid String,
  _id String,
  ts  UInt64,
  up     JSON(max_dynamic_paths = 128),
  custom JSON(max_dynamic_paths = 128),
  cmp    JSON(max_dynamic_paths = 32),
  sg     JSON(max_dynamic_paths = 128),
  c   UInt32,
  s   Float64,
  dur UInt32
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(fromUnixTimestamp64Milli(ts))
ORDER BY (a, e, ts)
SETTINGS index_granularity = 8192;
```

## Notes

- The writer uses `async_insert=1`, `wait_for_async_insert=1`, `optimize_on_insert=1`, and parallel parsing.
- Freshness is computed as `now64() - max(ts)` in milliseconds.
- Parts are read from `system.parts` to help you watch merge pressure.
