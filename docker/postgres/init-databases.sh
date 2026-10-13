#!/bin/bash
# Se ejecuta una sola vez, al crearse el volumen de Postgres.
# Una base de datos por servicio: ninguno lee la base de otro.
set -euo pipefail

for db in activation_db billing_db provisioning_db notification_db crm_db loyalty_db; do
  echo "Creando base $db"
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
       -c "CREATE DATABASE $db OWNER $POSTGRES_USER;"
done
