import 'dotenv/config';
import { Kafka, CompressionTypes, logLevel } from 'kafkajs';
import { customAlphabet } from 'nanoid';

const nanoid = customAlphabet('0123456789abcdefghijklmnopqrstuvwxyz', 16);

const BROKERS = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',');
const CLIENT_ID = process.env.KAFKA_CLIENT_ID ?? 'writer-kafka';
const TOPIC = process.env.KAFKA_TOPIC ?? 'clickhouse_ingest';

const INSERT_RATE = Number(process.env.INSERT_RATE ?? 10000);
const PRODUCER_LINGER_MS = Number(process.env.PRODUCER_LINGER_MS ?? 5);
const PRODUCER_ACKS = (process.env.PRODUCER_ACKS ?? 'all');

const COMP_MAP = { none: null, gzip: CompressionTypes.GZIP, snappy: CompressionTypes.Snappy, lz4: CompressionTypes.LZ4, zstd: CompressionTypes.ZSTD };
const PRODUCER_COMPRESSION = COMP_MAP[(process.env.PRODUCER_COMPRESSION ?? 'zstd').toLowerCase()] ?? CompressionTypes.ZSTD;

const KAFKA_BATCH_SIZE = Number(process.env.KAFKA_BATCH_SIZE ?? 5000);
const KAFKA_FLUSH_MS = Number(process.env.KAFKA_FLUSH_MS ?? 200);

const kafka = new Kafka({ clientId: CLIENT_ID, brokers: BROKERS, logLevel: logLevel.NOTHING });
const admin = kafka.admin();
const producer = kafka.producer({ allowAutoTopicCreation: false });

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

(async () => {
  console.log(`[producer] brokers=${BROKERS.join(',')} topic=${TOPIC} rate=${INSERT_RATE}/s batch=${KAFKA_BATCH_SIZE} flush_ms=${KAFKA_FLUSH_MS}`);

  await admin.connect();
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
  await admin.disconnect();

  await producer.connect();

  const queue = [];
  let lastFlush = Date.now();
  let sent = 0, sentSince = 0, lastStats = Date.now();

  async function flush(reason='periodic') {
    if (queue.length === 0) return;
    const msgs = queue.splice(0, queue.length).map(v => ({ value: v }));
    try {
      await producer.send({
        topic: TOPIC,
        acks: acksToNumber(PRODUCER_ACKS),
        compression: PRODUCER_COMPRESSION,
        messages: msgs
      });
      sent += msgs.length;
      sentSince += msgs.length;
    } catch (e) {
      console.error('[producer] send failed, re-queueing:', e?.message || e);
      queue.unshift(...msgs.map(m => m.value));
      await new Promise(r => setTimeout(r, 200));
    } finally {
      lastFlush = Date.now();
    }
  }

  const SLICE_MS = Math.max(1, PRODUCER_LINGER_MS || 5);
  const perTick = INSERT_RATE * (SLICE_MS / 1000);
  let acc = 0;

  setInterval(() => {
    acc += perTick;
    const emit = Math.floor(acc);
    acc -= emit;
    for (let i = 0; i < emit; i++) queue.push(JSON.stringify(makeEvent()));

    const elapsed = Date.now() - lastFlush;
    if (queue.length >= KAFKA_BATCH_SIZE) flush('batch_full');
    else if (elapsed >= KAFKA_FLUSH_MS)    flush('linger');

    const now = Date.now();
    if (now - lastStats >= 10000) {
      const rps = (sentSince * 1000) / (now - lastStats);
      console.log(`[producer] rps=${rps.toFixed(1)} total=${sent} qlen=${queue.length}`);
      lastStats = now;
      sentSince = 0;
    }
  }, SLICE_MS);

  async function shutdown() {
    console.log('[producer] shutting down…');
    try { await flush('shutdown'); } catch {}
    try { await producer.disconnect(); } catch {}
    process.exit(0);
  }
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
})();
