# Guión de la demo (≈ 15 min)

Pantalla dividida: **Demo UI** (http://localhost:5173) a la izquierda, **Kafka UI**
(http://localhost:8080) a la derecha. Antes de empezar: `docker compose up -d --build` y esperar a que
`docker compose ps` muestre todo `running`.

## Escenario 1 · Camino feliz — *fan-out*
1. UI: cliente `C-1234`, plan `FLOW-FULL`, **Sin fallo** → *Contratar plan* (responde `202 Accepted · act-0001 · PENDING`).
2. Se ve en la timeline: `ActivationRequested` → `BillingAccountCreated` → `ProvisioningCompleted` →
   `ActivationCompleted`, estado **ACTIVE**.
3. Mailhog (:8025): email "¡Tu plan FLOW-FULL ya está activo!".
4. Kafka UI → *Topics*: los mismos eventos, con clave `C-1234`. Un evento, varias reacciones.

## Escenario 2 · Fallo y compensación — *saga sin llamadas directas*
1. UI: **Falla provisioning** → contratar.
2. Timeline: `ProvisioningFailed`, `ActivationFailed`, `BillingAccountCancelled`; estado **FAILED**; email de error.
3. Mostrar que billing anuló la cuenta sin que nadie lo llamara:
   `docker compose exec postgres psql -U poc -d billing_db -c "select activation_id,status from billing_accounts"`.

## Escenario 3 · Servicio caído — *offsets*
```bash
docker compose stop notification-service
```
1. Crear 3 activaciones sin fallo.
2. Kafka UI → *Consumers* → `notification-svc`: **lag = 3** (el email no salió; Mailhog sin novedades).
3. `docker compose start notification-service` → llegan los 3 emails y el lag baja a 0. Nadie pierde mensajes.

## Escenario 4 · Escalar — *consumer group y particiones*
```bash
docker compose up -d --scale billing-service=2
```
Kafka UI → *Consumers* → `billing-svc`: las 3 particiones se reparten entre 2 miembros. Habrá una
pausa de unos segundos por el rebalanceo (explicarlo como parte de la demo; `KAFKA_SESSION_TIMEOUT_MS`
lo acorta). Crear activaciones con distintos clientes: se procesan igual, sin duplicados.

## Escenario 5 · Nuevo consumidor — *replay del log*
```bash
docker compose --profile loyalty up -d loyalty-service
docker compose logs -f loyalty-service
```
El servicio `loyalty` (group nuevo, `auto.offset.reset=earliest`) procesa **todo el historial** y suma
100 puntos por cada activación exitosa, sin tocar a ningún otro servicio. Con APIs directas esto no se puede.
Comprobar: `docker compose exec postgres psql -U poc -d loyalty_db -c "select * from loyalty_points"`.

## Extras útiles para la defensa
- **Idempotencia**: en Kafka UI → topic `activation.events` → reenviar (*Produce message*) el mismo JSON
  (mismo `eventId`): no se crea otro email. Los logs muestran "duplicado … se descarta".
- **DLQ**: producir en `activation.requested` el texto `{roto` → aparece en `activation.requested.dlq`
  con el motivo en los headers y el resto del flujo sigue.
- **Trazabilidad**: `select event_type, topic, kafka_partition, kafka_offset from event_log where correlation_id='act-0001' order by occurred_at;` en `crm_db`.
- **Reproceso del CRM (RF-10)**: `./docker/replay-crm.sh`.
- **Outbox**: `select status, count(*) from outbox group by 1;` en `activation_db`.

## Checklist previa (pruebas con Kafka real pendientes de correr con Docker)
- [ ] `docker compose up -d --build` levanta todo y `kafka-init` termina OK
- [ ] Escenario 1 y 2 en la UI
- [ ] Escenario 3: lag = 3 y luego 0
- [ ] Escenario 4: 3 particiones repartidas en 2 miembros
- [ ] Escenario 5: loyalty procesa el historial
- [ ] Grabar un video de respaldo de los 5 escenarios (riesgo "la demo depende de la red local")
