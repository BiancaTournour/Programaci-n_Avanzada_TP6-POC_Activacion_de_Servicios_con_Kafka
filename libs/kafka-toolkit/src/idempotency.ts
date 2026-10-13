import type { Pool } from 'pg';
import type { Queryable } from './database';

/**
 * Registra el eventId como procesado. Debe llamarse DENTRO de la misma transacción que el efecto.
 * Devuelve false si ya estaba registrado (evento duplicado: hay que descartarlo).
 * Si dos copias llegan a la vez, la segunda espera al COMMIT de la primera y recibe false.
 */
export async function markProcessed(client: Queryable, eventId: string, consumer: string): Promise<boolean> {
  const r = await client.query(
    'INSERT INTO processed_events (event_id, consumer) VALUES ($1, $2) ON CONFLICT DO NOTHING',
    [eventId, consumer],
  );
  return (r.rowCount ?? 0) > 0;
}

/**
 * Equivalente al índice TTL de Mongo: Postgres no expira filas solo, así que se purgan
 * periódicamente. 7 días = retención de los topics (un evento más viejo ya no puede llegar).
 */
export async function purgeOldRecords(pool: Pool, retentionDays = 7): Promise<void> {
  await pool.query(`DELETE FROM processed_events WHERE processed_at < now() - ($1 || ' days')::interval`, [String(retentionDays)]);
  const outbox = await pool.query(`SELECT to_regclass('outbox') AS t`);
  if (outbox.rows[0]?.t) {
    await pool.query(`DELETE FROM outbox WHERE status = 'SENT' AND sent_at < now() - ($1 || ' days')::interval`, [String(retentionDays)]);
  }
}
