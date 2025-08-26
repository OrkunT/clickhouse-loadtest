// maintenance.js
import 'dotenv/config';
import { createClient } from '@clickhouse/client';

const CH_URL  = process.env.CH_URL ?? process.env.CH_HOST ?? 'http://localhost:8123';
const CH_DB   = process.env.CH_DATABASE ?? 'default';
const CH_USER = process.env.CH_USER ?? 'default';
const CH_PASS = process.env.CH_PASSWORD ?? '';

const KAFKA_SRC_TABLE = process.env.KAFKA_SRC_TABLE ?? 'kafka_src';
const MV_NAME         = process.env.MV_NAME ?? 'mv_kafka_to_events';

// Optional: how many consumers to restore on resume (if you use --hard)
const RESTORE_CONSUMERS = Number(process.env.KAFKA_NUM_CONSUMERS ?? 1);

function usage() {
  console.log(`Usage:
  node maintenance.js pause [--hard]
  node maintenance.js resume [--hard]

Env:
  CH_URL / CH_HOST, CH_DATABASE, CH_USER, CH_PASSWORD
  KAFKA_SRC_TABLE (default: kafka_src)
  MV_NAME         (default: mv_kafka_to_events)
  KAFKA_NUM_CONSUMERS (default: 1 when resuming with --hard)`);
}

async function main() {
  const [,, cmd, flag] = process.argv;
  if (!cmd || !['pause', 'resume'].includes(cmd)) {
    usage();
    process.exit(1);
  }
  const HARD = flag === '--hard';

  const ch = createClient({ url: CH_URL, username: CH_USER, password: CH_PASS, database: CH_DB });

  try {
    // Ensure we’re in the right DB
    await ch.command({ query: `USE ${CH_DB}` });

    if (cmd === 'pause') {
      if (HARD) {
        console.log(`[pause] Setting kafka_num_consumers=0 on ${KAFKA_SRC_TABLE}…`);
        try {
          await ch.command({ query: `ALTER TABLE ${KAFKA_SRC_TABLE} MODIFY SETTING kafka_num_consumers = 0` });
        } catch (e) {
          console.warn(`[pause] WARN: couldn't set kafka_num_consumers=0 (${e?.message || e}). Continuing with MV detach.`);
        }
      }

      console.log(`[pause] Detaching MV ${MV_NAME}…`);
      await ch.command({ query: `DETACH MATERIALIZED VIEW IF EXISTS ${MV_NAME}` });

      console.log(`[pause] Done. ClickHouse will not consume/commit offsets while paused.`);
    }

    if (cmd === 'resume') {
      if (HARD) {
        console.log(`[resume] Restoring kafka_num_consumers=${RESTORE_CONSUMERS} on ${KAFKA_SRC_TABLE}…`);
        try {
          await ch.command({ query: `ALTER TABLE ${KAFKA_SRC_TABLE} MODIFY SETTING kafka_num_consumers = ${RESTORE_CONSUMERS}` });
        } catch (e) {
          console.warn(`[resume] WARN: couldn't set kafka_num_consumers (${e?.message || e}). Continuing.`);
        }
      }

      console.log(`[resume] Attaching MV ${MV_NAME}…`);
      await ch.command({ query: `ATTACH MATERIALIZED VIEW ${MV_NAME}` });

      console.log(`[resume] Done. ClickHouse will resume reading from Kafka and inserting.`);
    }
  } catch (err) {
    console.error('[maintenance] error:', err?.message || err);
    process.exit(1);
  } finally {
    try { await (await import('@clickhouse/client')).createClient; } catch {}
  }
}

main();
