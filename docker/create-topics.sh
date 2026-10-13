#!/bin/bash
# Crea los 4 topics de negocio y sus DLQ: 3 particiones, retención de 7 días.
# La lista debe coincidir con libs/contracts/src/topics.ts (hay un test que lo verifica).
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:29092}"
KT=/opt/kafka/bin/kafka-topics.sh
PARTITIONS=3
RETENTION_MS=604800000   # 7 días

TOPICS=(
  activation.requested
  billing.events
  provisioning.events
  activation.events
)

create() {
  "$KT" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$1" --partitions "$PARTITIONS" --replication-factor 1 \
    --config "retention.ms=$RETENTION_MS"
}

for t in "${TOPICS[@]}"; do
  create "$t"
  create "$t.dlq"
done

echo "--- Topics creados ---"
"$KT" --bootstrap-server "$BOOTSTRAP" --list
