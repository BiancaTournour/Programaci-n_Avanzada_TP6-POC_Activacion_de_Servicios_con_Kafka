# Arquitectura (versión PostgreSQL)

Complementa el documento de arquitectura del TP. Todo lo que el PDF define (topics, eventos,
flujos, estados, requerimientos) se respeta; este archivo documenta **cómo se implementó** y
**en qué difiere** por usar PostgreSQL en lugar de MongoDB.

## 1. Componentes

```
Demo UI --POST /activations--> activation-api --(outbox)--> Kafka
   ^                               ^   |                       |
   '------- WebSocket -------------'   |        +--------------+--------------+----------------+
                                       |        v              v              v                v
                                  PostgreSQL   billing    provisioning   notification    crm-analytics (+ loyalty)
                                 (1 base por   (billing_db) (provisioning_db) (notification_db)  (crm_db)
                                   servicio)
```

| Servicio | Consume (group) | Publica | Base |
|---|---|---|---|
| activation-api | `billing.events`, `provisioning.events` (`activation-api`) | `activation.requested`, `activation.events` | `activation_db` |
| billing | `activation.requested`, `activation.events` (`billing-svc`) | `billing.events` | `billing_db` |
| provisioning | `activation.requested` (`provisioning-svc`) | `provisioning.events` | `provisioning_db` |
| notification | `activation.events` (`notification-svc`) | — | `notification_db` |
| crm-analytics | los 4 topics (`crm-analytics`) | — | `crm_db` |
| loyalty (demo) | `activation.events` (`loyalty-svc`) | — | `loyalty_db` |

Coreografía con agregador: billing y provisioning reaccionan solos; `activation-api` solo junta
resultados (`ActivationsService.applyResult`) y decide `ACTIVE` / `FAILED`.

## 2. De MongoDB a PostgreSQL

| En el PDF (Mongo) | Acá (Postgres) | Nota |
|---|---|---|
| Replica set de 1 nodo "necesario para transacciones" | No hace falta | Postgres tiene transacciones ACID de fábrica |
| Colección `activations` (`steps`, `history` embebidos) | Tabla `activations` con columnas `jsonb` | `id` = activationId (`act-0001`, por secuencia) |
| `billing_accounts`, `provisioning_orders`, `notifications` | Tablas homónimas | `activation_id` único (PK o `UNIQUE`) |
| `event_log` (eventId único, correlationId indexado) | Tabla `event_log` | PK `event_id`, índice por `correlation_id` |
| `outbox` (índice status + createdAt) | Tabla `outbox` + índice parcial `WHERE status='PENDING'` | |
| `processed_events` con **índice TTL de 7 días** | Tabla `processed_events` + **purga cada hora** | Postgres no expira filas solo; ver `purgeOldRecords` |
| Una base por servicio | Una base por servicio en un mismo contenedor | `ensureDatabase()` la crea si falta |

## 3. Decisiones técnicas (sección 08 del PDF)

1. **Idempotencia** — `EventProcessor` inserta `(eventId, consumer)` en `processed_events` **dentro
   de la misma transacción** que el efecto de negocio. Duplicado → se descarta y se confirma el offset.
   Si dos copias llegan a la vez, la segunda espera el COMMIT de la primera (PK única) y se descarta.
2. **Reintentos y DLQ** — 1 intento + **3 reintentos** con espera de 1 s, 2 s y 4 s. Al agotarlos se
   publica el mensaje original en `<topic>.dlq` con headers `x-error-reason`, `x-original-topic`,
   `x-original-partition`, `x-original-offset`, `x-failed-consumer`, `x-attempts`, `x-failed-at`, y se
   sigue con el siguiente (no se bloquea la partición). Un mensaje **inválido** (no parsea o le faltan
   campos) va **directo** a la DLQ: reintentar no lo arreglaría. Los fallos simulados
   (`simulateFailure`) son errores de negocio: se publican como `BillingFailed`/`ProvisioningFailed`.
3. **Outbox transaccional** — se implementa **desde el principio** en los 3 servicios que publican
   (el PDF lo marca "fase 4" y publica directo en las fases 1-3). Como Postgres da transacciones sin
   configuración extra, no hay razón para dejar el riesgo de "guardo y me caigo antes de publicar".
   El estado y el evento se guardan en una transacción; `OutboxRelay` lee cada 500 ms, publica en
   orden y marca `SENT`. Un `pg_try_advisory_xact_lock` hace que, con varias instancias del mismo
   servicio, publique solo una a la vez (conserva el orden). Garantía: *al menos una vez*.
4. **Cliente Kafka** — `@nestjs/microservices` (`ClientKafka` para publicar, `@EventPattern` para
   consumir) sobre kafkajs. Todo el acceso a Kafka está en `libs/kafka-toolkit`
   (`publisher.ts`, `kafka-options.ts`) para poder cambiarlo por el cliente de Confluent.

Otras decisiones: **commit de offset manual**, siempre después de procesar (`run.autoCommit=false`);
`postfixId: ''` para que el group se vea como `billing-svc` en Kafka UI (Nest agrega `-server` por
defecto); `auto.offset.reset=earliest` por defecto para que un group nuevo (o un miembro nuevo en un
rebalanceo) no se salte mensajes — RNF-05.

## 4. Caso borde de la compensación

`activation.requested` y `activation.events` son topics distintos: Kafka no garantiza el orden
entre ellos. Si `ActivationFailed` llega a billing **antes** de que cree la cuenta:

1. billing no encuentra cuenta → anota la activación en `activation_failures`;
2. cuando después procesa `ActivationRequested`, crea la cuenta, ve la anotación y la **anula
   enseguida** (publica `BillingAccountCreated` y `BillingAccountCancelled`).

Ambos handlers toman un `pg_advisory_xact_lock` por `activationId`, así que el caso también es
correcto con 2 instancias de billing procesando en paralelo (escenario 4). Está cubierto por un test.

## 5. Estados y agregación

`PENDING → IN_PROGRESS → ACTIVE | FAILED`. Con billing **y** provisioning OK → `ACTIVE` +
`ActivationCompleted`. Con el **primer** fallo → `FAILED` + `ActivationFailed`. Resultados posteriores
a un estado final solo se registran en `history` (p. ej. un `ProvisioningCompleted` tardío tras un
`BillingFailed`). La fila de la activación se lee con `FOR UPDATE`: dos resultados simultáneos se serializan.

> Nota: en un fallo de **billing**, provisioning (que corre en paralelo y no sabe del fallo) puede
> completarse igual; el PDF define compensación solo para billing, así que esa orden de
> aprovisionamiento no se revierte. Es una limitación de la POC, no un bug.

## 6. Reproceso del CRM (RF-10)

`docker/replay-crm.sh` detiene crm-analytics, vacía `event_log` **y `processed_events`** (si no, la
idempotencia descartaría todo), resetea los offsets del group a `earliest` y lo levanta.
