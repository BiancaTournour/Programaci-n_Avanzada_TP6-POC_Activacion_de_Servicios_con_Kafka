#!/bin/bash
# RF-10: crm-analytics reprocesa TODO el historial desde el inicio del log.
# Detiene el servicio, vacía su copia local (event_log + processed_events, si no la idempotencia
# descartaría los eventos), vuelve el offset del group a "earliest" y lo levanta de nuevo.
set -euo pipefail
cd "$(dirname "$0")/.."

docker compose stop crm-analytics-service
docker compose exec -T postgres psql -U poc -d crm_db -c "TRUNCATE event_log, processed_events;"
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:29092 --group crm-analytics --reset-offsets --to-earliest --all-topics --execute
docker compose start crm-analytics-service
echo "Listo: crm-analytics está releyendo los topics desde el offset 0."
