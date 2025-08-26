#!/usr/bin/env bash
set -euo pipefail

# ========================
# Usage:
#   ./set_partitions.sh <topic> <num_partitions>
#
# Example:
#   ./set_partitions.sh clickhouse_ingest 3
# ========================

TOPIC=${1:-}
PARTITIONS=${2:-}

if [[ -z "$TOPIC" || -z "$PARTITIONS" ]]; then
  echo "Usage: $0 <topic> <num_partitions>"
  exit 1
fi

KAFKA_DIR=${KAFKA_DIR:-/opt/kafka}
BROKER=${BROKER:-localhost:9092}

echo "==> Altering topic '$TOPIC' to partitions=$PARTITIONS"
$KAFKA_DIR/bin/kafka-topics.sh \
  --bootstrap-server "$BROKER" \
  --alter \
  --topic "$TOPIC" \
  --partitions "$PARTITIONS"

echo "==> Done. Current topics/partitions:"
$KAFKA_DIR/bin/kafka-topics.sh \
  --bootstrap-server "$BROKER" \
  --describe --topic "$TOPIC"
