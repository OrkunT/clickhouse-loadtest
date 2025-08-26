import 'dotenv/config';
import { createClient } from '@clickhouse/client';

const CH_URL   = process.env.CH_URL ?? process.env.CH_HOST ?? 'http://localhost:8123';
const CH_DB    = process.env.CH_DATABASE ?? 'default';
const CH_USER  = process.env.CH_USER ?? 'default';
const CH_PASS  = process.env.CH_PASSWORD ?? '';

const TARGET_TABLE    = process.env.TARGET_TABLE    ?? 'drill_events_test';
const KAFKA_SRC_TABLE = process.env.KAFKA_SRC_TABLE ?? 'kafka_src';
const MV_NAME         = process.env.MV_NAME         ?? 'mv_kafka_to_events';

const KAFKA_BROKERS   = process.env.KAFKA_BROKERS   ?? 'localhost:9092';
const KAFKA_TOPIC     = process.env.KAFKA_TOPIC     ?? 'clickhouse_ingest';
const KAFKA_GROUP     = process.env.KAFKA_GROUP     ?? 'ch_group';

const STREAM_FLUSH_INTERVAL_MS = Number(process.env.STREAM_FLUSH_INTERVAL_MS ?? 20000);
const KAFKA_MAX_BLOCK_SIZE     = Number(process.env.KAFKA_MAX_BLOCK_SIZE     ?? 100000);
const KAFKA_POLL_MAX_BATCH_SIZE= Number(process.env.KAFKA_POLL_MAX_BATCH_SIZE?? 100000);
const KAFKA_POLL_TIMEOUT_MS    = Number(process.env.KAFKA_POLL_TIMEOUT_MS    ?? 1000);

// Optional safety: 1 => don't drop src+MV (idempotent)
const SAFE_MODE = Number(process.env.SAFE_MODE ?? 0) === 1;

const ch = createClient({ url: CH_URL, username: CH_USER, password: CH_PASS, database: CH_DB });

const DDL_TARGET = `
CREATE TABLE IF NOT EXISTS ${TARGET_TABLE}
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
  dur UInt32,

  INDEX idx_ts_minmax ts TYPE minmax GRANULARITY 1,
  INDEX idx_uid_bloom uid TYPE bloom_filter(0.01) GRANULARITY 64
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(fromUnixTimestamp64Milli(ts))
ORDER BY (a, e, ts)
SETTINGS index_granularity = 8192
`;

const DDL_KAFKA_SRC = `
CREATE TABLE ${SAFE_MODE ? 'IF NOT EXISTS ' : ''}${KAFKA_SRC_TABLE}
(
  a   String,
  e   String,
  uid String,
  did String,
  lsid String,
  _id String,
  ts  UInt64,

  up     String,
  custom String,
  cmp    String,
  sg     String,

  c   UInt32,
  s   Float64,
  dur UInt32
)
ENGINE = Kafka
SETTINGS
  kafka_broker_list = '${KAFKA_BROKERS}',
  kafka_topic_list = '${KAFKA_TOPIC}',
  kafka_group_name = '${KAFKA_GROUP}',
  kafka_format = 'JSONEachRow',
  kafka_num_consumers = 1,
  kafka_max_block_size = ${KAFKA_MAX_BLOCK_SIZE},
  kafka_poll_max_batch_size = ${KAFKA_POLL_MAX_BATCH_SIZE},
  kafka_poll_timeout_ms = ${KAFKA_POLL_TIMEOUT_MS}
`;

// NOTE: No SETTINGS here to avoid the ENGINE+TO parser complaint on some versions
const DDL_MV_MINIMAL = `
CREATE MATERIALIZED VIEW ${SAFE_MODE ? 'IF NOT EXISTS ' : ''}${MV_NAME}
TO ${TARGET_TABLE}
AS
SELECT
  a, e, uid, did, lsid, _id, ts,
  JSONParse(up)     AS up,
  JSONParse(custom) AS custom,
  JSONParse(cmp)    AS cmp,
  JSONParse(sg)     AS sg,
  c, s, dur
FROM ${KAFKA_SRC_TABLE}
`;

async function run(label, query) {
  console.log(`[setup] ${label}`);
  await ch.command({ query });
}

async function trySetMVFlushInterval() {
  // Best-effort: newer CH allows this; older builds may reject it.
  try {
    await ch.command({
      query: `ALTER TABLE ${MV_NAME} MODIFY SETTING stream_flush_interval_ms = ${STREAM_FLUSH_INTERVAL_MS}`
    });
    console.log(`[setup] set MV stream_flush_interval_ms=${STREAM_FLUSH_INTERVAL_MS}`);
  } catch (e) {
    console.warn(`[setup] WARN: couldn't set stream_flush_interval_ms on MV (${e?.message || e}).`);
    console.warn(`[setup]      The MV will flush based on incoming Kafka blocks (kafka_max_block_size/poll cadence).`);
  }
}

async function main() {
  console.log(`[setup] CH=${CH_URL}/${CH_DB}`);
  console.log(`[setup] TARGET=${TARGET_TABLE} SRC=${KAFKA_SRC_TABLE} MV=${MV_NAME}`);
  console.log(`[setup] Kafka: brokers=${KAFKA_BROKERS}, topic=${KAFKA_TOPIC}, group=${KAFKA_GROUP}`);
  console.log(`[setup] Intended MV flush ~${STREAM_FLUSH_INTERVAL_MS} ms`);
  console.log(`[setup] SAFE_MODE=${SAFE_MODE ? 1 : 0}`);

  await ch.command({ query: 'SET allow_experimental_object_type = 1' });
  await run(`ensure target table: ${TARGET_TABLE}`, DDL_TARGET);

  if (!SAFE_MODE) {
    console.log(`[setup] dropping MV if exists: ${MV_NAME}`);
    await ch.command({ query: `DROP VIEW IF EXISTS ${MV_NAME}` }).catch(()=>{});
    console.log(`[setup] dropping Kafka src if exists: ${KAFKA_SRC_TABLE}`);
    await ch.command({ query: `DROP TABLE IF EXISTS ${KAFKA_SRC_TABLE}` }).catch(()=>{});
  }

  await run(`create Kafka source: ${KAFKA_SRC_TABLE}`, DDL_KAFKA_SRC);
  await run(`create MV (no SETTINGS in CREATE to avoid parser issue): ${MV_NAME}`, DDL_MV_MINIMAL);
  await trySetMVFlushInterval();

  console.log('[setup] done.');
  await ch.close();
}

main().catch(async (e) => {
  console.error('[setup] error:', e?.message || e);
  try { await ch.close(); } catch {}
  process.exit(1);
});
