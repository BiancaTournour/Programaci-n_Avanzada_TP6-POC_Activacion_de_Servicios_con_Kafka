import { Logger, type OnModuleDestroy, type OnModuleInit } from '@nestjs/common';
import { partitionKey, topicOf, type DomainEvent } from '@poc/contracts';
import type { Pool } from 'pg';
import type { Queryable } from './database';
import type { EventPublisher } from './publisher';

/**
 * Guarda el evento en la tabla outbox. Se llama con el client de la MISMA transacción que
 * cambia el estado de negocio: o se guardan los dos o ninguno.
 */
export async function enqueueEvent(client: Queryable, event: DomainEvent): Promise<void> {
  await client.query(
    `INSERT INTO outbox (event_id, topic, key, payload) VALUES ($1, $2, $3, $4::jsonb)`,
    [event.eventId, topicOf(event.eventType), partitionKey(event), JSON.stringify(event)],
  );
}

const BATCH = 50;
const RELAY_LOCK = 727275;

/**
 * Proceso aparte que lee el outbox cada 500 ms, publica los pendientes en orden y los marca
 * como enviados. Garantiza "al menos una vez": si se cae entre publicar y marcar, republica
 * (los consumidores son idempotentes).
 */
export class OutboxRelay implements OnModuleInit, OnModuleDestroy {
  private readonly log = new Logger(OutboxRelay.name);
  private timer?: NodeJS.Timeout;
  private busy = false;

  constructor(
    private readonly pool: Pool,
    private readonly publisher: EventPublisher,
    private readonly intervalMs = 500,
  ) {}

  onModuleInit(): void {
    this.timer = setInterval(() => void this.tick(), this.intervalMs);
    this.log.log(`Relay del outbox activo (cada ${this.intervalMs} ms)`);
  }

  onModuleDestroy(): void {
    if (this.timer) clearInterval(this.timer);
  }

  async tick(): Promise<number> {
    if (this.busy) return 0;
    this.busy = true;
    let total = 0;
    try {
      let n: number;
      do {
        n = await this.flushBatch();
        total += n;
      } while (n === BATCH);
    } catch (err) {
      this.log.warn(`No se pudo publicar el outbox (se reintenta): ${(err as Error).message}`);
    } finally {
      this.busy = false;
    }
    return total;
  }

  /** Publica hasta BATCH eventos pendientes. Devuelve cuántos se enviaron. */
  private async flushBatch(): Promise<number> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      // Solo una instancia del servicio publica a la vez: conserva el orden del outbox.
      const lock = await client.query('SELECT pg_try_advisory_xact_lock($1) AS ok', [RELAY_LOCK]);
      if (!lock.rows[0].ok) {
        await client.query('ROLLBACK');
        return 0;
      }
      const { rows } = await client.query(
        `SELECT id, topic, key, payload FROM outbox WHERE status = 'PENDING' ORDER BY id LIMIT ${BATCH}`,
      );
      const sent: string[] = [];
      let failure: Error | undefined;
      for (const row of rows) {
        try {
          await this.publisher.publish({ topic: row.topic, key: row.key, value: row.payload });
          sent.push(row.id);
        } catch (err) {
          failure = err as Error;
          await client.query(`UPDATE outbox SET attempts = attempts + 1, last_error = $2 WHERE id = $1`, [row.id, failure.message]);
          break; // se conserva el orden: no se publica nada después del que falló
        }
      }
      if (sent.length) {
        await client.query(`UPDATE outbox SET status = 'SENT', sent_at = now() WHERE id = ANY($1::bigint[])`, [sent]);
      }
      await client.query('COMMIT');
      if (failure) throw failure;
      return sent.length;
    } catch (err) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw err;
    } finally {
      client.release();
    }
  }
}
