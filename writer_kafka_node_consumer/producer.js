import 'dotenv/config';
import { Kafka, CompressionTypes, logLevel } from 'kafkajs';
import { customAlphabet } from 'nanoid';

const nanoid = customAlphabet('0123456789abcdefghijklmnopqrstuvwxyz', 16);

// ---------- env
const BROKERS = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',');
const CLIENT_ID = process.env.KAFKA_CLIENT_ID ?? 'writer-kafka';
const TOPIC = process.env.KAFKA_TOPIC ?? 'clickhouse_ingest';
const CREATE_TOPIC = Number(process.env.KAFKA_CREATE_TOPIC ?? 1) === 1;

const INSERT_RATE = Number(process.env.INSERT_RATE ?? 10000);
const PRODUCER_LINGER_MS = Number(process.env.PRODUCER_LINGER_MS ?? 5);
const PRODUCER_ACKS = (process.env.PRODUCER_ACKS ?? 'all'); // 0|1|all

// only gzip is built-in; snappy/lz4 need extra plugins; zstd is not available
const RAW_COMPRESSION = String(process.env.PRODUCER_COMPRESSION ?? 'gzip').toLowerCase();

function resolveCompression(name) {
  switch (name) {
    case 'none':   return { type: null, label: 'none' };
    case 'gzip':   return { type: CompressionTypes.GZIP, label: 'gzip' };
    // If you install extra codecs, you can enable these:
    // case 'snappy': return { type: CompressionTypes.Snappy, label: 'snappy' };
    // case 'lz4':    return { type: CompressionTypes.LZ4, label: 'lz4' };
    case 'zstd':
      console.warn('[producer] ZSTD not supported in KafkaJS without a codec plugin; falling back to gzip.');
      return { type: CompressionTypes.GZIP, label: 'gzip(fallback-from-zstd)' };
    default:
      console.warn(`[producer] Unknown compression "${name}"; falling back to gzip.`);
      return { type: CompressionTypes.GZIP, label: 'gzip(fallback)' };
  }
}
const { type: PRODUCER_COMPRESSION, label: COMPRESSION_LABEL } = resolveCompression(RAW_COMPRESSION);

const KAFKA_BATCH_SIZE = Number(process.env.KAFKA_BATCH_SIZE ?? 5000);
const KAFKA_FLUSH_MS = Number(process.env.KAFKA_FLUSH_MS ?? 200);

// ---------- kafka
const kafka = new Kafka({
  clientId: CLIENT_ID,
  brokers: BROKERS,
  logLevel: logLevel.NOTHING
});

const admin = kafka.admin();
const producer = kafka.producer({
  allowAutoTopicCreation: false,
  // KafkaJS batches internally; linger is approximated by our local buffer timers.
});

// ---------- helpers
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

function acksToNumber(v) {
  if (v === 'all') return -1;
  const n = Number(v);
  return Number.isFinite(n) ? n : -1;
}

// ---------- main
(async () => {
  console.log(
    `[producer] brokers=${BROKERS.join(',')} topic=${TOPIC} rate=${INSERT_RATE}/s ` +
    `batch=${KAFKA_BATCH_SIZE} flush_ms=${KAFKA_FLUSH_MS} acks=${PRODUCER_ACKS} compression=${COMPRESSION_LABEL}`
  );

  await admin.connect();
  if (CREATE_TOPIC) {
    try {
      await admin.createTopics({
        waitForLeaders: true,
        topics: [{ topic: TOPIC, numPartitions: 1, replicationFactor: 1 }]
      });
      console.log(`[producer] topic ensured: ${TOPIC}`);
    } catch (e) {
      if (!/already exists/i.test(String(e?.message))) {
        console.warn('[producer] createTopics error:', e?.message || e);
      }
    }
  }
  await admin.disconnect();

  await producer.connect();

  // batching queue for kafka.send
  const queue = [];
  let lastFlush = Date.now();
  let sent = 0;
  let lastStats = Date.now();
  let sentSince = 0;

  async function flush(reason='periodic') {
    if (queue.length === 0) return;
    const msgs = queue.splice(0, queue.length).map(v => ({ value: v }));
    try {
      await producer.send({
        topic: TOPIC,
        acks: acksToNumber(PRODUCER_ACKS),
        compression: PRODUCER_COMPRESSION, // may be null (no compression) or GZIP
        messages: msgs
      });
      sent += msgs.length;
      sentSince += msgs.length;
    } catch (e) {
      console.error('[producer] send failed, re-queueing:', e?.message || e);
      // put back in front
      queue.unshift(...msgs.map(m => m.value));
      await new Promise(r => setTimeout(r, 200));
    } finally {
      lastFlush = Date.now();
    }
  }

  // pace generator @ INSERT_RATE rows/sec using 100ms slices with fractional carry
  const SLICE_MS = Math.max(1, PRODUCER_LINGER_MS || 5);
  const perTick = INSERT_RATE * (SLICE_MS / 1000);
  let acc = 0;

  setInterval(() => {
    // generate
    acc += perTick;
    const emit = Math.floor(acc);
    acc -= emit;

    for (let i = 0; i < emit; i++) {
      queue.push(JSON.stringify(makeEvent()));
    }

    // flush policy
    const elapsed = Date.now() - lastFlush;
    if (queue.length >= KAFKA_BATCH_SIZE) {
      flush('batch_full');
    } else if (elapsed >= KAFKA_FLUSH_MS) {
      flush('linger');
    }

    // stats every ~10s
    const now = Date.now();
    if (now - lastStats >= 10000) {
      const rps = (sentSince * 1000) / (now - lastStats);
      console.log(`[producer] rps=${rps.toFixed(1)} total=${sent} qlen=${queue.length}`);
      lastStats = now;
      sentSince = 0;
    }
  }, SLICE_MS);

  // graceful shutdown
  async function shutdown() {
    console.log('[producer] shutting down…');
    try { await flush('shutdown'); } catch {}
    try { await producer.disconnect(); } catch {}
    process.exit(0);
  }
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
})();
