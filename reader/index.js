// reader/index.js
import 'dotenv/config';
import { createClient } from '@clickhouse/client';

// ---- env & defaults
const CH_URL   = process.env.CH_URL      ?? 'http://localhost:8123';
const CH_DB    = process.env.CH_DATABASE ?? 'default';
const CH_USER  = process.env.CH_USER     ?? 'default';
const CH_PASS  = process.env.CH_PASSWORD ?? '';
const TABLE    = process.env.TABLE_NAME  ?? 'drill_events_test';

const QPM       = Number(process.env.QUERIES_PER_MIN ?? 30);   // e.g. 30 = every ~2s
const WINDOW_S  = Number(process.env.QUERY_WINDOW_SEC ?? 60);  // lookback for “rows_last_window”
const PERIOD_MS = Math.max(1000, Math.floor(60000 / Math.max(1, QPM)));

// ---- clickhouse client
const client = createClient({
  url: CH_URL,
  username: CH_USER,
  password: CH_PASS,
  database: CH_DB,
});

// ---- observability table (if missing)
const OBS_DDL = `
CREATE TABLE IF NOT EXISTS observability_rt
(
  table_name                   String,
  source                       LowCardinality(String),
  ts                           DateTime64(3),
  write_rps                    Float64,
  written_total                UInt64,
  batch_size                   UInt32,
  flush_ms                     UInt32,
  read_window_s                UInt32,
  rows_read_window             UInt64,
  read_lag_ms                  UInt64,
  active_parts                 UInt64,
  parts_in_latest_partition    UInt64,
  latest_partition             String,
  parts_to_delay_insert        UInt64,
  parts_to_throw_insert        UInt64,
  max_parts_in_total           UInt64
)
ENGINE = MergeTree
ORDER BY (table_name, ts, source)
SETTINGS index_granularity = 8192
`;

async function ensureTables() {
  await client.command({ query: OBS_DDL });
}

// ---- probe queries
function freshnessQuery() {
  return `
    WITH toUnixTimestamp64Milli(now64()) AS now_ms
    SELECT
      now_ms,
      ifNull(max(ts), now_ms) AS max_ts_ms,
      now_ms - max_ts_ms      AS lag_ms,
      countIf(ts >= now_ms - toUInt64(${WINDOW_S * 1000})) AS rows_last_window
    FROM ${TABLE}
  `;
}

const metaQ = `
  SELECT
    /* total active parts */
    (SELECT count()
     FROM system.parts
     WHERE database = currentDatabase()
       AND table = {table:String}
       AND active) AS active_parts,

    /* parts inside most recently modified partition */
    (SELECT count()
     FROM system.parts
     WHERE database = currentDatabase()
       AND table = {table:String}
       AND active
       AND partition_id = (
         SELECT argMax(partition_id, modification_time)
         FROM system.parts
         WHERE database = currentDatabase()
           AND table = {table:String}
           AND active
       )
    ) AS parts_in_latest_partition,

    /* latest partition id */
    (SELECT ifNull(argMax(partition_id, modification_time), '')
     FROM system.parts
     WHERE database = currentDatabase()
       AND table = {table:String}
       AND active) AS latest_partition,

    /* NEW: totals over active parts */
    (SELECT toUInt64(sum(rows))
     FROM system.parts
     WHERE database = currentDatabase()
       AND table = {table:String}
       AND active) AS total_rows_active,

    (SELECT toUInt64(sum(bytes_on_disk))
     FROM system.parts
     WHERE database = currentDatabase()
       AND table = {table:String}
       AND active) AS total_bytes_active,

    /* thresholds */
    (SELECT toUInt64(value) FROM system.merge_tree_settings WHERE name = 'parts_to_delay_insert') AS parts_to_delay_insert,
    (SELECT toUInt64(value) FROM system.merge_tree_settings WHERE name = 'parts_to_throw_insert')  AS parts_to_throw_insert,
    (SELECT toUInt64(value) FROM system.merge_tree_settings WHERE name = 'max_parts_in_total')     AS max_parts_in_total
`;

// ---- one probe cycle
let prevTotals = { rows: 0n, bytes: 0n, parts: 0n };

async function probeOnce() {
  const t0 = Date.now();
  try {
    // A) freshness
    const fres = await (await client.query({ query: freshnessQuery(), format: 'JSONEachRow' })).json();
    const {
      lag_ms = 0,
      rows_last_window = 0,
    } = fres?.[0] ?? {};

    // B) parts + totals + thresholds
    const meta = await (await client.query({
      query: metaQ,
      query_params: { table: TABLE },
      format: 'JSONEachRow',
    })).json();

    const {
      active_parts = 0,
      parts_in_latest_partition = 0,
      latest_partition = '',
      total_rows_active = 0,
      total_bytes_active = 0,
      parts_to_delay_insert = 0,
      parts_to_throw_insert = 0,
      max_parts_in_total = 0,
    } = meta?.[0] ?? {};

    // deltas (to spot merges/compactions)
    const dRows  = BigInt(total_rows_active)  - prevTotals.rows;
    const dBytes = BigInt(total_bytes_active) - prevTotals.bytes;
    const dParts = BigInt(active_parts)       - prevTotals.parts;
    prevTotals = {
      rows:  BigInt(total_rows_active),
      bytes: BigInt(total_bytes_active),
      parts: BigInt(active_parts),
    };

    // C) insert a row into observability_rt (reader)
    await client.query({
      query: `
        INSERT INTO observability_rt
        SELECT
          {table:String} AS table_name,
          'reader'       AS source,
          now64(3)       AS ts,
          toFloat64(0)   AS write_rps,
          toUInt64(0)    AS written_total,
          toUInt32(0)    AS batch_size,
          toUInt32(0)    AS flush_ms,
          toUInt32({window_s:UInt32})   AS read_window_s,
          toUInt64({rows:UInt64})       AS rows_read_window,
          toUInt64({lag:UInt64})        AS read_lag_ms,
          toUInt64({parts:UInt64})      AS active_parts,
          toUInt64({parts_latest:UInt64}) AS parts_in_latest_partition,
          {latest:String}               AS latest_partition,
          toUInt64({p_delay:UInt64})    AS parts_to_delay_insert,
          toUInt64({p_throw:UInt64})    AS parts_to_throw_insert,
          toUInt64({p_total:UInt64})    AS max_parts_in_total
      `,
      query_params: {
        table: TABLE,
        window_s: WINDOW_S,
        rows: String(rows_last_window),
        lag: String(lag_ms),
        parts: String(active_parts),
        parts_latest: String(parts_in_latest_partition),
        latest: latest_partition ?? '',
        p_delay: String(parts_to_delay_insert),
        p_throw: String(parts_to_throw_insert),
        p_total: String(max_parts_in_total),
      },
    });

    const took = Date.now() - t0;
    console.log(
      `[reader] took=${took}ms lag=${lag_ms}ms rows_${WINDOW_S}s=${rows_last_window} ` +
      `parts=${active_parts} (Δ${dParts >= 0n ? '+' : ''}${dParts}) ` +
      `total_rows=${total_rows_active} (Δ${dRows >= 0n ? '+' : ''}${dRows}) ` +
      `total_bytes=${total_bytes_active} (Δ${dBytes >= 0n ? '+' : ''}${dBytes}) ` +
      `latest_part=${latest_partition}`
    );
  } catch (err) {
    console.error('[reader] probe error:', err?.message || err);
  }
}

(async function main() {
  console.log(`reader starting → every ${PERIOD_MS}ms (≈ ${QPM} qpm), window=${WINDOW_S}s, table=${TABLE}`);
  await ensureTables();
  await probeOnce();                       // fire once immediately
  setInterval(probeOnce, PERIOD_MS);       // and then on schedule
})();

// graceful exit
async function shutdown() {
  try { await client.close(); } catch {}
  process.exit(0);
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
