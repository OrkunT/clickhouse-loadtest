import 'dotenv/config';
import { createClient } from '@clickhouse/client';
import { customAlphabet } from 'nanoid';

const nanoid = customAlphabet('0123456789abcdefghijklmnopqrstuvwxyz', 16);

// ---- config
const CH_URL   = process.env.CH_URL ?? process.env.CH_HOST ?? 'http://localhost:8123';
const CH_DB    = process.env.CH_DATABASE ?? 'default';
const CH_USER  = process.env.CH_USER ?? 'default';
const CH_PASS  = process.env.CH_PASSWORD ?? '';
const TABLE    = process.env.TABLE_NAME ?? 'drill_events_test';

const INSERT_RATE = Number(process.env.INSERT_RATE ?? 4);      // rows/sec
const BATCH_SIZE  = Number(process.env.BATCH_SIZE ?? 80);      // rows per flush
const FLUSH_MS    = Number(process.env.FLUSH_MS ?? 20000);     // flush interval ms

const ASYNC_INSERT          = Number(process.env.ASYNC_INSERT ?? 1);
const WAIT_FOR_ASYNC_INSERT = Number(process.env.WAIT_FOR_ASYNC_INSERT ?? 1);

// --- optional server-side coalescing knobs
const ASYNC_INSERT_BUSY_TIMEOUT_MS = process.env.ASYNC_INSERT_BUSY_TIMEOUT_MS
  ? Number(process.env.ASYNC_INSERT_BUSY_TIMEOUT_MS)
  : undefined;                          // e.g., 30000 (30s)
const MIN_INSERT_BLOCK_SIZE_ROWS = process.env.MIN_INSERT_BLOCK_SIZE_ROWS
  ? Number(process.env.MIN_INSERT_BLOCK_SIZE_ROWS)
  : undefined;                          // e.g., 100000
const MIN_INSERT_BLOCK_SIZE_BYTES = process.env.MIN_INSERT_BLOCK_SIZE_BYTES
  ?? undefined;                         // e.g., '32M' (keep as string)

// how often to re-print server settings & async log snapshots
const DEBUG_SETTINGS_INTERVAL_MS = Number(process.env.DEBUG_SETTINGS_INTERVAL_MS ?? 15000);

// ---- clickhouse client
const client = createClient({
  url: CH_URL,
  username: CH_USER,
  password: CH_PASS,
  database: CH_DB,
});

// ---- DDL
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

const DDL_OBS = `
CREATE TABLE IF NOT EXISTS observability_rt
(
  table_name String,
  source LowCardinality(String),
  ts DateTime64(3),
  -- writer-side
  write_rps Float64,
  written_total UInt64,
  batch_size UInt32,
  flush_ms UInt32,
  -- reader-side
  read_window_s UInt32,
  rows_read_window UInt64,
  read_lag_ms UInt64,
  -- parts + thresholds snapshot
  active_parts UInt32,
  parts_in_latest_partition UInt32,
  latest_partition String,
  parts_to_delay_insert UInt64,
  parts_to_throw_insert UInt64,
  max_parts_in_total UInt64
)
ENGINE = MergeTree
ORDER BY (table_name, source, ts)
`;

// ---- helpers
async function ensureTables() {
  await client.command({ query: 'SET allow_experimental_object_type = 1' });
  await client.command({ query: DDL_EVENTS });
  await client.command({ query: DDL_OBS });
}

// build per-insert CH settings (adds server-side knobs if provided)
function buildCHSettings() {
  const s = {
    async_insert: ASYNC_INSERT,
    wait_for_async_insert: WAIT_FOR_ASYNC_INSERT,
    input_format_parallel_parsing: 1,
    optimize_on_insert: 1,
  };
  if (ASYNC_INSERT_BUSY_TIMEOUT_MS !== undefined) {
    s.async_insert_busy_timeout_ms = ASYNC_INSERT_BUSY_TIMEOUT_MS;
  }
  if (MIN_INSERT_BLOCK_SIZE_ROWS !== undefined) {
    s.min_insert_block_size_rows = MIN_INSERT_BLOCK_SIZE_ROWS;
  }
  if (MIN_INSERT_BLOCK_SIZE_BYTES !== undefined) {
    s.min_insert_block_size_bytes = MIN_INSERT_BLOCK_SIZE_BYTES; // keep string like '32M'
  }
  return s;
}

async function printEffectiveSettings(prefix = 'debug') {
  // Ask the server what it sees for THIS request (settings are per-query)
  const settings = buildCHSettings();
  try {
    const rs = await client.query({
      query: `
        SELECT
          toUInt8(getSetting('async_insert'))                      AS async_insert,
          toUInt8(getSetting('wait_for_async_insert'))             AS wait_for_async_insert,
          toUInt64(getSetting('async_insert_busy_timeout_ms'))     AS async_insert_busy_timeout_ms,
          toUInt64(getSetting('min_insert_block_size_rows'))       AS min_insert_block_size_rows,
          toUInt64(getSetting('min_insert_block_size_bytes'))      AS min_insert_block_size_bytes
      `,
      format: 'JSONEachRow',
      clickhouse_settings: settings,
    });
    const [row] = await rs.json();
    console.log(`[${prefix}] local clickhouse_settings ->`, settings);
    console.log(`[${prefix}] server getSetting(...)     ->`, row);
  } catch (e) {
    console.warn(`[${prefix}] could not read getSetting():`, e?.message || e);
  }

  // Also print a small snapshot of async/sync insert logs to see coalescing
  try {
    const asyncLog = await (
      await client.query({
        query: `
          SELECT event_time, rows
          FROM system.asynchronous_insert_log
          WHERE database = currentDatabase() AND table = {tbl:String}
          ORDER BY event_time DESC LIMIT 5
        `,
        query_params: { tbl: TABLE },
        format: 'JSONEachRow',
      })
    ).json();
    console.log(`[${prefix}] async_log last5 ->`, asyncLog);
  } catch {}

  try {
    const syncLog = await (
      await client.query({
        query: `
          SELECT event_time, written_rows
          FROM system.query_log
          WHERE current_database = currentDatabase()
            AND query_kind='Insert'
            AND positionCaseInsensitive(query, 'INSERT INTO {tbl:String}') > 0
            AND type='QueryFinish'
          ORDER BY event_time DESC LIMIT 5
        `,
        query_params: { tbl: TABLE },
        format: 'JSONEachRow',
      })
    ).json();
    console.log(`[${prefix}] query_log last5  ->`, syncLog);
  } catch {}
}

// ---- data gen
function makeEvent() {
  const nowMs = BigInt(Date.now());
  return {
    a: 'app_1',
    e: Math.random() < 0.5 ? 'click' : 'view',
    uid: `u_${nanoid()}`,
    did: `d_${nanoid()}`,
    lsid: `s_${nanoid()}`,
    _id: nanoid(),
    ts: Number(nowMs),

    up: { name: 'Alex', plan: 'pro', age: 29 },
    custom: { feature: 'A', screen: 'home', ref: nanoid() },
    cmp: { id: 'cmp_42', channel: 'email' },
    sg: { group: 'beta' },

    c: Math.floor(Math.random() * 10),
    s: Math.random() * 100,
    dur: Math.floor(Math.random() * 1000),
  };
}

// ---- batching
const queue = [];
let lastFlushAt = Date.now();
let inserting = false;
let written = 0;
let batches = 0;

async function upsertWriterTelemetry() {
  const q = `
  INSERT INTO observability_rt (
    table_name, source, ts,
    write_rps, written_total, batch_size, flush_ms,
    read_window_s, rows_read_window, read_lag_ms,
    active_parts, parts_in_latest_partition, latest_partition,
    parts_to_delay_insert, parts_to_throw_insert, max_parts_in_total
  )
  SELECT
    '${TABLE}' AS table_name,
    'writer'   AS source,
    now64(3)   AS ts,
    ${INSERT_RATE}::Float64 AS write_rps,
    ${written}::UInt64      AS written_total,
    ${BATCH_SIZE}::UInt32   AS batch_size,
    ${FLUSH_MS}::UInt32     AS flush_ms,
    0, 0, 0,
    /* parts snapshot */
    (SELECT count() FROM system.parts
      WHERE database = currentDatabase() AND table = '${TABLE}' AND active) AS active_parts,
    (SELECT count() FROM system.parts
      WHERE database = currentDatabase() AND table = '${TABLE}' AND active
        AND partition = (SELECT argMax(partition, modification_time)
                         FROM system.parts
                         WHERE database = currentDatabase() AND table = '${TABLE}' AND active)
    ) AS parts_in_latest_partition,
    (SELECT ifNull(argMax(partition, modification_time), '') 
      FROM system.parts WHERE database = currentDatabase() AND table = '${TABLE}' AND active) AS latest_partition,
    /* thresholds */
    (SELECT toUInt64(value) FROM system.merge_tree_settings WHERE name='parts_to_delay_insert')  AS parts_to_delay_insert,
    (SELECT toUInt64(value) FROM system.merge_tree_settings WHERE name='parts_to_throw_insert')  AS parts_to_throw_insert,
    (SELECT toUInt64(value) FROM system.merge_tree_settings WHERE name='max_parts_in_total')     AS max_parts_in_total
  `;
  await client.command({ query: q });
}

async function flush(reason = 'periodic') {
  if (inserting || queue.length === 0) return;

  const batch = queue.splice(0, queue.length);
  inserting = true;
  const started = Date.now();

  try {
    const cs = buildCHSettings(); // capture for debug if needed
    await client.insert({
      table: TABLE,
      values: batch,
      format: 'JSONEachRow',
      clickhouse_settings: cs,
    });

    written += batch.length;
    batches += 1;
    const took = Date.now() - started;
    console.log(
      `[flush] ${reason} wrote=${batch.length} total=${written} batches=${batches} took=${took}ms qlen=${queue.length}`
    );

    // update observability
    await upsertWriterTelemetry();

  } catch (err) {
    console.error('[flush] insert failed, re-queueing:', err?.message || err);
    queue.unshift(...batch);
    await new Promise(r => setTimeout(r, 500));
  } finally {
    inserting = false;
    lastFlushAt = Date.now();
  }
}

// pace producer at INSERT_RATE rows/sec, tick every 100ms with fractional carry
const SLICE_MS = 100;
const perTick = INSERT_RATE * (SLICE_MS / 1000);
let acc = 0;

setInterval(() => {
  acc += perTick;
  const emit = Math.floor(acc);
  acc -= emit;
  for (let i = 0; i < emit; i++) queue.push(makeEvent());

  const since = Date.now() - lastFlushAt;
  if (queue.length >= BATCH_SIZE) flush('batch_full');
  else if (since >= FLUSH_MS) flush('timer');
}, SLICE_MS);

// periodic telemetry (rows written last minute via async or sync paths)
setInterval(async () => {
  try {
    const rs = await client.query({
      query: `
        WITH now() AS n
        SELECT
          (SELECT toUInt64(sum(rows)) FROM system.asynchronous_insert_log
           WHERE database=currentDatabase() AND table='${TABLE}' AND event_time >= n - toIntervalMinute(1)) AS async_rows,
          (SELECT toUInt64(sum(written_rows)) FROM system.query_log
           WHERE current_database=currentDatabase()
             AND query_kind='Insert'
             AND positionCaseInsensitive(query, 'INSERT INTO ${TABLE}') > 0
             AND type='QueryFinish'
             AND event_time >= n - toIntervalMinute(1)) AS sync_rows
      `,
      format: 'JSONEachRow'
    });
    const [{ async_rows = 0, sync_rows = 0 } = {}] = await rs.json();
    console.log(`[telemetry] last_1m async_rows=${async_rows} sync_rows=${sync_rows}`);
  } catch {}
}, 60000);

// periodic debug: what settings are actually in effect + last inserts
//setInterval(() => printEffectiveSettings('debug'), DEBUG_SETTINGS_INTERVAL_MS);

// graceful exit
async function shutdown() {
  console.log('shutting down: flushing…');
  await flush('shutdown');
  await client.close().catch(() => {});
  process.exit(0);
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

// boot
(async () => {
  console.log(
    `writer starting → rate=${INSERT_RATE}/s, batch=${BATCH_SIZE}, flush_ms=${FLUSH_MS}, ` +
    `async=${ASYNC_INSERT}, wait=${WAIT_FOR_ASYNC_INSERT}, ` +
    `timeout_ms=${ASYNC_INSERT_BUSY_TIMEOUT_MS ?? 'unset'}, ` +
    `min_rows=${MIN_INSERT_BLOCK_SIZE_ROWS ?? 'unset'}, min_bytes=${MIN_INSERT_BLOCK_SIZE_BYTES ?? 'unset'}`
  );
  await ensureTables();
  console.log('tables ready; writing into:', TABLE);

  // print once at boot too
  await printEffectiveSettings('boot');
})();
