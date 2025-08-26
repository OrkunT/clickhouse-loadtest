// consumer_ch_sink.js
import 'dotenv/config';
import { Kafka, logLevel } from 'kafkajs';
import { createClient } from '@clickhouse/client';

// ---- env
const BROKERS = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',');
const CLIENT_ID = process.env.KAFKA_CLIENT_ID ?? 'writer-kafka';
const GROUP_ID = process.env.KAFKA_GROUP_ID ?? 'writer-kafka-sink';
const TOPIC = process.env.KAFKA_TOPIC ?? 'clickhouse_ingest';

const CH_URL   = process.env.CH_URL ?? process.env.CH_HOST ?? 'http://localhost:8123';
const CH_DB    = process.env.CH_DATABASE ?? 'default';
const CH_USER  = process.env.CH_USER ?? 'default';
const CH_PASS  = process.env.CH_PASSWORD ?? '';
const TABLE    = process.env.TABLE_NAME ?? 'drill_events_test';

const BUFFER_SECONDS = Number(process.env.BUFFER_SECONDS ?? 20);
const MAX_BUFFER_ROWS = Number(process.env.MAX_BUFFER_ROWS ?? 1_000_000);

const ASYNC_INSERT          = Number(process.env.ASYNC_INSERT ?? 1);
const WAIT_FOR_ASYNC_INSERT = Number(process.env.WAIT_FOR_ASYNC_INSERT ?? 1);
const ASYNC_INSERT_BUSY_TIMEOUT_MS = process.env.ASYNC_INSERT_BUSY_TIMEOUT_MS ? Number(process.env.ASYNC_INSERT_BUSY_TIMEOUT_MS) : undefined;
const MIN_INSERT_BLOCK_SIZE_ROWS   = process.env.MIN_INSERT_BLOCK_SIZE_ROWS   ? Number(process.env.MIN_INSERT_BLOCK_SIZE_ROWS)   : undefined;
const MIN_INSERT_BLOCK_SIZE_BYTES  = process.env.MIN_INSERT_BLOCK_SIZE_BYTES  ?? undefined;

// ---- clients
const kafka = new Kafka({ clientId: CLIENT_ID, brokers: BROKERS, logLevel: logLevel.NOTHING });
const consumer = kafka.consumer({ groupId: GROUP_ID, allowAutoTopicCreation: false /* important */, retry: { retries: 8 } });
const ch = createClient({ url: CH_URL, username: CH_USER, password: CH_PASS, database: CH_DB });

// ---- CH DDL (same schema)
const DDL_EVENTS = `
CREATE TABLE IF NOT EXISTS ${TABLE}
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

async function ensureTables() {
  await ch.command({ query: 'SET allow_experimental_object_type = 1' });
  await ch.command({ query: DDL_EVENTS });
}

function buildCHSettings() {
  const s = {
    async_insert: ASYNC_INSERT,
    wait_for_async_insert: WAIT_FOR_ASYNC_INSERT,
    input_format_parallel_parsing: 1,
    optimize_on_insert: 1,
  };
  if (ASYNC_INSERT_BUSY_TIMEOUT_MS !== undefined) s.async_insert_busy_timeout_ms = ASYNC_INSERT_BUSY_TIMEOUT_MS;
  if (MIN_INSERT_BLOCK_SIZE_ROWS !== undefined)   s.min_insert_block_size_rows = MIN_INSERT_BLOCK_SIZE_ROWS;
  if (MIN_INSERT_BLOCK_SIZE_BYTES !== undefined)  s.min_insert_block_size_bytes = MIN_INSERT_BLOCK_SIZE_BYTES;
  return s;
}

// ---- buffering + offset tracking
let buffer = [];                                     // accumulated rows (objects)
const lastOffsets = new Map();                       // key: `${topic}:${partition}` -> lastOffset string
let inFlush = false;
let written = 0, since = 0, lastStats = Date.now();

// commit helper (commit NEXT offset per Kafka semantics)
async function commitAllUpTo() {
  const commits = [];
  for (const [key, off] of lastOffsets.entries()) {
    const [topic, partStr] = key.split(':');
    const next = (BigInt(off) + 1n).toString();
    commits.push({ topic, partition: Number(partStr), offset: next });
  }
  if (commits.length) {
    await consumer.commitOffsets(commits);
  }
}

async function flush(reason = 'timer') {
  if (inFlush || buffer.length === 0) return;
  inFlush = true;
  const batch = buffer;           // take snapshot
  buffer = [];

  const offsetsSnapshot = new Map(lastOffsets); // commit these only if insert succeeds

  try {
    const t0 = Date.now();
    await ch.insert({
      table: TABLE,
      values: batch,
      format: 'JSONEachRow',
      clickhouse_settings: buildCHSettings(),
    });
    const took = Date.now() - t0;
    written += batch.length;
    since += batch.length;
    console.log(`[sink] flush=${reason} rows=${batch.length} total=${written} took=${took}ms`);

    // commit AFTER successful insert
    await commitAllUpTo();
  } catch (e) {
    console.error('[sink] insert failed; messages will be reprocessed (no commit):', e?.message || e);
    // Restore the batch to buffer (front) so we try again next cycle
    buffer.unshift(...batch);
    // Roll back lastOffsets to pre-flush snapshot (not strictly necessary since we didn't commit, but keeps logs tidy)
    for (const [k, v] of offsetsSnapshot.entries()) lastOffsets.set(k, v);
    await new Promise(r => setTimeout(r, 500));
  } finally {
    inFlush = false;
  }
}

(async () => {
  console.log(`[sink] brokers=${BROKERS.join(',')} topic=${TOPIC} group=${GROUP_ID} buffer=${BUFFER_SECONDS}s cap=${MAX_BUFFER_ROWS}`);
  await ensureTables();
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: false });

  // Fixed 20s (configurable) timer
  const interval = setInterval(() => flush('timer'), Math.max(1000, BUFFER_SECONDS * 1000));

  // Stats
  const telem = setInterval(() => {
    const now = Date.now();
    if (now - lastStats >= 10000) {
      const rps = (since * 1000) / (now - lastStats);
      console.log(`[sink] rps=${rps.toFixed(1)} total=${written} qlen=${buffer.length}`);
      lastStats = now;
      since = 0;
    }
  }, 2000);

  // IMPORTANT: disable auto-commit for at-least-once
  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic, partition, message, heartbeat }) => {
      try {
        buffer.push(JSON.parse(message.value.toString('utf8')));

        // track last offset seen per partition
        const key = `${topic}:${partition}`;
        lastOffsets.set(key, message.offset);

        // safety cap
        if (!inFlush && buffer.length >= MAX_BUFFER_ROWS) {
          await flush('max_buffer_rows');
        }

        // keep session alive in high-throughput
        if (Math.random() < 0.01) await heartbeat();
      } catch (e) {
        console.error('[sink] bad message (skipping, no commit):', e?.message || e);
      }
    }
  });

  async function shutdown() {
    console.log('[sink] shutting down…');
    clearInterval(interval);
    clearInterval(telem);
    try { await flush('shutdown'); } catch {}
    try { await consumer.disconnect(); } catch {}
    try { await ch.close(); } catch {}
    process.exit(0);
  }
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
})();
