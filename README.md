# activation-poc

POC de **activación de servicios con Kafka** · NestJS · React · PostgreSQL
(Programación Avanzada 2026 · Licenciatura en Sistemas de Información · FCyT).

Un cliente contrata un plan desde la UI; `activation-api` publica **un único evento**
(`ActivationRequested`) y de ahí en adelante facturación, aprovisionamiento, notificación,
CRM y fidelización reaccionan solos. Ningún servicio llama a otro.

## Levantar todo (RNF-01)

```bash
docker compose up -d --build
```

| URL | Qué es |
|---|---|
| http://localhost:5173 | **Demo UI** (formulario + timeline en vivo) |
| http://localhost:8080 | **Kafka UI** (topics, offsets, consumer groups, lag) |
| http://localhost:8025 | **Mailhog** (emails de bienvenida / error) |
| http://localhost:3000/activations | API REST de `activation-api` |
| `localhost:15432` | PostgreSQL (`poc` / `poc`, una base por servicio) |

Reset completo: `docker compose down -v`. Guión de la demo: [`docs/demo-script.md`](docs/demo-script.md).

## Estructura

```
apps/
  activation-api/          REST + WebSocket + agregador de la saga          (puerto 3000)
  billing-service/         crea la cuenta; la anula ante ActivationFailed
  provisioning-service/    aprovisiona (retardo 1-3 s); puede fallar a pedido
  notification-service/    email a Mailhog por ActivationCompleted/Failed
  crm-analytics-service/   registra TODOS los eventos; no publica nada
  loyalty-service/         escenario 5: nuevo consumidor, lee el historial (profile "loyalty")
  demo-ui/                 React + Vite + socket.io-client
libs/
  contracts/               sobre común, eventos versionados, topics, groups (fuente de verdad)
  kafka-toolkit/           idempotencia, reintentos, DLQ, outbox, commit manual
tests/                     saga completa + cableado Nest
docker/                    create-topics.sh, Dockerfiles, replay-crm.sh, init de Postgres
docs/                      architecture.md, demo-script.md
```

## Desarrollo y tests

Requisitos: Node ≥ 20, pnpm ≥ 9.

```bash
pnpm install
pnpm build            # contracts -> kafka-toolkit -> servicios -> UI
pnpm test:libs        # contracts (sin dependencias) + kafka-toolkit (necesita Postgres)

# Tests que usan Postgres (con la infraestructura de Compose levantada):
export TEST_DATABASE_URL=postgres://poc:poc@localhost:15432/toolkit_test
export TEST_PG_URL=postgres://poc:poc@localhost:15432
pnpm test             # libs + saga completa + cableado de Nest
```

Sin esas variables, los tests que necesitan Postgres se **omiten** (no fallan).

## Qué se cambió respecto del documento (MongoDB → PostgreSQL)

El PDF habla de MongoDB; este proyecto usa **PostgreSQL**. Equivalencias en
[`docs/architecture.md`](docs/architecture.md#2-de-mongodb-a-postgresql). Lo principal:
transacciones nativas (no hace falta replica set), `outbox`/`processed_events` como tablas,
`jsonb` para `steps`/`history`, y una purga periódica en lugar del índice TTL de Mongo.

## Criterios de aceptación (sección 11 del PDF)

| Criterio | Cómo se verifica |
|---|---|
| `docker compose up` levanta todo sin pasos manuales | Compose con healthchecks; las bases y tablas se crean solas al arrancar |
| Los 5 escenarios en < 15 min | `docs/demo-script.md` (manual, con Docker) |
| Reenviar un evento no duplica cuentas ni emails | test automático *"reenviar todos los eventos…"* |
| Un mensaje inválido va a la DLQ y el flujo sigue | test automático *"un mensaje inválido va a la DLQ…"* |
| Todo se rastrea por `correlationId` | test automático + tabla `event_log` del CRM |

> **Estado de la verificación:** la lógica (saga, compensación, idempotencia, reintentos, DLQ, outbox,
> cableado de NestJS, HTTP) está probada con Postgres real y un bus en memoria en lugar de Kafka.
> La integración con un **broker Kafka real** (rebalanceos, lag, escenarios 3-5) hay que
> ejecutarla con Docker; ver la checklist en `docs/demo-script.md`.
