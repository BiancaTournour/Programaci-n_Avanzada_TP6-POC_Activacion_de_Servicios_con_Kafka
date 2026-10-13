import { Logger } from '@nestjs/common';
import { Pool, type PoolClient } from 'pg';
import { sleep } from './env';

const log = new Logger('Database');

/** Cualquier cosa con `.query` (Pool o PoolClient). */
export interface Queryable {
  query: PoolClient['query'];
}

async function waitFor<T>(what: string, fn: () => Promise<T>, attempts = 30): Promise<T> {
  let last: unknown;
  for (let i = 1; i <= attempts; i++) {
    try {
      return await fn();
    } catch (err) {
      last = err;
      log.warn(`${what} no disponible (intento ${i}/${attempts}): ${(err as Error).message}`);
      await sleep(1000);
    }
  }
  throw last;
}

/**
 * Crea la base de datos del servicio si no existe. Evita depender de que
 * docker/postgres/init-databases.sh haya corrido (solo corre con un volumen nuevo).
 */
export async function ensureDatabase(databaseUrl: string): Promise<void> {
  const url = new URL(databaseUrl);
  const dbName = decodeURIComponent(url.pathname.replace(/^\//, ''));
  if (!/^[A-Za-z0-9_]+$/.test(dbName)) throw new Error(`Nombre de base inválido: ${dbName}`);
  url.pathname = '/postgres';
  const admin = new Pool({ connectionString: url.toString(), max: 1 });
  try {
    await waitFor('PostgreSQL', () => admin.query('SELECT 1'));
    const r = await admin.query('SELECT 1 FROM pg_database WHERE datname = $1', [dbName]);
    if (!r.rowCount) {
      try {
        await admin.query(`CREATE DATABASE "${dbName}"`);
        log.log(`Base ${dbName} creada`);
      } catch (err) {
        if ((err as { code?: string }).code !== '42P04') throw err; // ya existe (carrera entre instancias)
      }
    }
  } finally {
    await admin.end();
  }
}

export function createPool(databaseUrl: string): Pool {
  const pool = new Pool({ connectionString: databaseUrl, max: 10 });
  pool.on('error', (err) => log.error(`Error en conexión inactiva: ${err.message}`));
  return pool;
}

/** Ejecuta `fn` dentro de una transacción; hace ROLLBACK si lanza. */
export async function withTransaction<T>(pool: Pool, fn: (client: PoolClient) => Promise<T>): Promise<T> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const result = await fn(client);
    await client.query('COMMIT');
    return result;
  } catch (err) {
    await client.query('ROLLBACK').catch(() => undefined);
    throw err;
  } finally {
    client.release();
  }
}

/** Bloqueo asesor transaccional por clave de texto: serializa handlers de una misma activación. */
export async function lockKey(client: Queryable, key: string): Promise<void> {
  await client.query('SELECT pg_advisory_xact_lock(hashtext($1))', [key]);
}

const COMMON_SCHEMA = `
CREATE TABLE IF NOT EXISTS processed_events (
  event_id     text        NOT NULL,
  consumer     text        NOT NULL,
  processed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (event_id, consumer)
);
CREATE INDEX IF NOT EXISTS processed_events_at_idx ON processed_events (processed_at);
`;

const OUTBOX_SCHEMA = `
CREATE TABLE IF NOT EXISTS outbox (
  id         bigserial   PRIMARY KEY,
  event_id   text        NOT NULL,
  topic      text        NOT NULL,
  key        text        NOT NULL,
  payload    jsonb       NOT NULL,
  status     text        NOT NULL DEFAULT 'PENDING',
  attempts   int         NOT NULL DEFAULT 0,
  last_error text,
  created_at timestamptz NOT NULL DEFAULT now(),
  sent_at    timestamptz
);
CREATE INDEX IF NOT EXISTS outbox_pending_idx ON outbox (status, created_at) WHERE status = 'PENDING';
`;

/**
 * Crea las tablas comunes (processed_events, outbox) y las propias del servicio.
 * Usa un advisory lock para que varias instancias arrancando a la vez (docker compose --scale)
 * no choquen ejecutando el mismo DDL.
 */
export async function initSchema(pool: Pool, opts: { outbox: boolean; extra?: string[] }): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query('SELECT pg_advisory_lock(727274)');
    await client.query(COMMON_SCHEMA);
    if (opts.outbox) await client.query(OUTBOX_SCHEMA);
    for (const ddl of opts.extra ?? []) await client.query(ddl);
  } finally {
    await client.query('SELECT pg_advisory_unlock(727274)').catch(() => undefined);
    client.release();
  }
}
