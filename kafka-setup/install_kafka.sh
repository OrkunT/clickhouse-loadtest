#!/usr/bin/env bash
set -euo pipefail

# =============================
# Tunables (override via env)
# =============================
KAFKA_VERSION="${KAFKA_VERSION:-3.9.1}"
SCALA_VERSION="${SCALA_VERSION:-2.13}"
JAVA_LTS="${JAVA_LTS:-17}"                      # 17 (default) or 21
TOPIC_NAME="${TOPIC_NAME:-clickhouse_ingest}"
PARTITIONS="${PARTITIONS:-1}"
REPLICATION_FACTOR=1                            # single node -> must be 1
KAFKA_USER="${KAFKA_USER:-kafka}"
KAFKA_DIR="${KAFKA_DIR:-/opt/kafka}"
KAFKA_DATA="${KAFKA_DATA:-/var/lib/kafka}"
KAFKA_PORT="${KAFKA_PORT:-9092}"                # client / broker
KAFKA_CTRL_PORT="${KAFKA_CTRL_PORT:-9093}"      # controller quorum RPC
ADVERTISED_HOST="${ADVERTISED_HOST:-localhost}" # change to server IP/DNS for remote clients
KAFKA_HEAP_OPTS="${KAFKA_HEAP_OPTS:--Xms512m -Xmx1g}"

# Helper: run a command as kafka user (works whether or not we're root and whether sudo exists)
run_as_kafka() {
  local CMD="$*"
  if command -v sudo >/dev/null 2>&1; then
    sudo -u "$KAFKA_USER" bash -lc "$CMD"
  else
    su -s /bin/bash -c "$CMD" "$KAFKA_USER"
  fi
}

# Detect sudo prefix for system ops
if [ "$(id -u)" -eq 0 ]; then SUDO=""; else SUDO="sudo"; fi

echo "==> Installing prerequisites (JDK ${JAVA_LTS})"
$SUDO apt-get update -y
$SUDO apt-get install -y "openjdk-${JAVA_LTS}-jdk" curl wget tar rsync

# Java sanity (ensure >=17)
if command -v java >/dev/null 2>&1; then
  JV=$(java -version 2>&1 | awk -F\" '/version/ {print $2}')
  JMAJ=$(echo "$JV" | awk -F. '{print $1}')
  if [ "$JMAJ" -lt 17 ]; then
    echo "Detected Java $JV (<17). Upgrading to OpenJDK ${JAVA_LTS}..."
    $SUDO apt-get install -y "openjdk-${JAVA_LTS}-jdk"
  fi
fi

echo "==> Kafka ${KAFKA_VERSION} requires Scala ${SCALA_VERSION} build (no Scala runtime install needed)."

echo "==> Creating kafka user and directories"
if ! id -u "$KAFKA_USER" >/dev/null 2>&1; then
  $SUDO useradd -m -s /bin/bash "$KAFKA_USER"
fi
$SUDO mkdir -p "$KAFKA_DIR" "$KAFKA_DATA"
$SUDO chown -R "$KAFKA_USER:$KAFKA_USER" "$KAFKA_DIR" "$KAFKA_DATA"

echo "==> Downloading Kafka ${KAFKA_VERSION} (Scala ${SCALA_VERSION})"
cd /tmp
if [ ! -f "kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" ]; then
  wget -q "https://downloads.apache.org/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
fi
tar -xzf "kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
$SUDO rsync -a "kafka_${SCALA_VERSION}-${KAFKA_VERSION}/" "$KAFKA_DIR/"
$SUDO chown -R "$KAFKA_USER:$KAFKA_USER" "$KAFKA_DIR"
rm -rf "kafka_${SCALA_VERSION}-${KAFKA_VERSION}"*

echo "==> Writing KRaft server.properties"
cat <<EOF | $SUDO tee "$KAFKA_DIR/config/kraft/server.properties" >/dev/null
# --- Single-node KRaft (broker + controller) ---
process.roles=broker,controller
node.id=1

# Listeners
listeners=PLAINTEXT://0.0.0.0:${KAFKA_PORT},CONTROLLER://0.0.0.0:${KAFKA_CTRL_PORT}
advertised.listeners=PLAINTEXT://${ADVERTISED_HOST}:${KAFKA_PORT}
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
inter.broker.listener.name=PLAINTEXT
controller.listener.names=CONTROLLER

# Storage
log.dirs=${KAFKA_DATA}

# Controller quorum
controller.quorum.voters=1@${ADVERTISED_HOST}:${KAFKA_CTRL_PORT}

# Single-node sensible defaults
num.partitions=${PARTITIONS}
offsets.topic.replication.factor=${REPLICATION_FACTOR}
transaction.state.log.replication.factor=${REPLICATION_FACTOR}
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0

# QoL
auto.create.topics.enable=false
delete.topic.enable=true
EOF

echo "==> Formatting KRaft storage"
CLUSTER_ID="$($KAFKA_DIR/bin/kafka-storage.sh random-uuid)"
echo "Cluster ID: $CLUSTER_ID"
run_as_kafka "$KAFKA_DIR/bin/kafka-storage.sh format -t '$CLUSTER_ID' -c '$KAFKA_DIR/config/kraft/server.properties'"

echo "==> Creating systemd unit"
cat <<EOF | $SUDO tee /etc/systemd/system/kafka.service >/dev/null
[Unit]
Description=Apache Kafka ${KAFKA_VERSION} (KRaft)
After=network.target

[Service]
Type=simple
User=${KAFKA_USER}
Environment="KAFKA_HEAP_OPTS=${KAFKA_HEAP_OPTS}"
ExecStart=${KAFKA_DIR}/bin/kafka-server-start.sh ${KAFKA_DIR}/config/kraft/server.properties
Restart=on-failure
RestartSec=5
LimitNOFILE=100000

[Install]
WantedBy=multi-user.target
EOF

echo "==> Starting Kafka service"
$SUDO systemctl daemon-reload
$SUDO systemctl enable kafka
$SUDO systemctl restart kafka || (echo "Kafka failed to start. Check logs: journalctl -u kafka -n 200 --no-pager" && exit 1)
sleep 3
$SUDO systemctl --no-pager --full status kafka || true

echo "==> Creating topic '${TOPIC_NAME}' (partitions=${PARTITIONS}, rf=${REPLICATION_FACTOR})"
set +e
"$KAFKA_DIR/bin/kafka-topics.sh" \
  --bootstrap-server "${ADVERTISED_HOST}:${KAFKA_PORT}" \
  --create --topic "${TOPIC_NAME}" \
  --partitions "${PARTITIONS}" \
  --replication-factor "${REPLICATION_FACTOR}" 2>/tmp/kafka-topic-create.err
RC=$?
set -e
if [ $RC -ne 0 ]; then
  if grep -qi "already exists" /tmp/kafka-topic-create.err; then
    echo "Topic '${TOPIC_NAME}' already exists — continuing."
  else
    echo "Topic creation failed:"; cat /tmp/kafka-topic-create.err; exit 1
  fi
fi

echo "==> Topics:"
"$KAFKA_DIR/bin/kafka-topics.sh" --bootstrap-server "${ADVERTISED_HOST}:${KAFKA_PORT}" --list

cat <<INFO

? Kafka ${KAFKA_VERSION} installed (KRaft mode), running under systemd.
   Service logs:   journalctl -u kafka -f
   Broker address: ${ADVERTISED_HOST}:${KAFKA_PORT}
   Data dir:       ${KAFKA_DATA}
   Topic created:  ${TOPIC_NAME}

If clients are on another machine, re-run with ADVERTISED_HOST=<server-ip-or-dns>.
INFO
