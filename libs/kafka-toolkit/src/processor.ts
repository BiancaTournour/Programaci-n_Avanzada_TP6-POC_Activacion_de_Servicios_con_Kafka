import { Logger, type OnModuleDestroy, type OnModuleInit } from '@nestjs/common';
import type { KafkaContext } from '@nestjs/microservices';
import { dlqOf, parseEvent, partitionKey, type DomainEvent } from '@poc/contracts';
import type { Pool, PoolClient } from 'pg';
import { enqueueEvent } from './outbox';
import { markProcessed, purgeOldRecords } from './idempotency';
import type { EventPublisher } from './publisher';
import { sleep } from './env';

export interface MessageMeta {
  topic: string;
  partition: number;
  offset: string;
  key: string | null;
}

/** Lo que recibe el handler de negocio: todo su trabajo con la base ocurre en `client` (una transacción). */
export interface HandlerContext {
  client: PoolClient;
  meta: MessageMeta;
  /** Encola un evento en el outbox, dentro de la misma transacción. */
  enqueue(event: DomainEvent): Promise<void>;
  /** Código que debe ejecutarse solo si la transacción hizo COMMIT (ej.: avisar por WebSocket). */
  afterCommit(fn: () => void | Promise<void>): void;
}

export type EventHandler = (event: DomainEvent, ctx: HandlerContext) => Promise<void>;
export type ProcessOutcome = 'processed' | 'duplicate' | 'dlq';

export interface ProcessorOptions {
  /** Nombre del consumer group; se guarda en processed_events. */
  consumer: string;
  /** Esperas entre reintentos. Por defecto 1 s, 2 s, 4 s => 1 intento + 3 reintentos. */
  retryDelaysMs?: number[];
  retentionDays?: number;
}

export const DEFAULT_RETRY_DELAYS_MS = [1000, 2000, 4000];

/**
 * Envuelve a cada handler de negocio con las garantías de los requerimientos:
 *  - RNF-03 idempotencia: eventId en processed_events, misma transacción que el efecto.
 *  - RNF-04 reintentos con espera creciente y DLQ, sin frenar la partición.
 *  - RNF-05 commit de offset manual, SOLO después de procesar.
 */
export class EventProcessor implements OnModuleInit, OnModuleDestroy {
  private readonly log = new Logger(EventProcessor.name);
  private readonly retryDelays: number[];
  private purgeTimer?: NodeJS.Timeout;

  constructor(
    private readonly pool: Pool,
    private readonly publisher: EventPublisher,
    private readonly options: ProcessorOptions,
  ) {
    this.retryDelays = options.retryDelaysMs ?? DEFAULT_RETRY_DELAYS_MS;
  }

  onModuleInit(): void {
    const purge = () => purgeOldRecords(this.pool, this.options.retentionDays ?? 7).catch((e) => this.log.warn(`Purga fallida: ${e.message}`));
    void purge();
    this.purgeTimer = setInterval(purge, 60 * 60 * 1000);
    this.purgeTimer.unref();
  }

  onModuleDestroy(): void {
    if (this.purgeTimer) clearInterval(this.purgeTimer);
  }

  /** Punto de entrada desde un @EventPattern. Nunca lanza por errores del handler. */
  handle(kafka: KafkaContext, data: unknown, handler: EventHandler): Promise<ProcessOutcome> {
    const message = kafka.getMessage();
    const topic = kafka.getTopic();
    const partition = kafka.getPartition();
    const consumer = kafka.getConsumer();
    const meta: MessageMeta = {
      topic,
      partition,
      offset: message.offset,
      key: message.key == null ? null : String(message.key),
    };
    const commit = () =>
      consumer.commitOffsets([{ topic, partition, offset: (BigInt(message.offset) + 1n).toString() }]);
    return this.processMessage(meta, data, handler, commit);
  }

  /** Núcleo, independiente de Nest/Kafka para poder probarlo. */
  async processMessage(
    meta: MessageMeta,
    value: unknown,
    handler: EventHandler,
    commit: () => Promise<void>,
  ): Promise<ProcessOutcome> {
    let event: DomainEvent;
    try {
      event = parseEvent(value);
    } catch (err) {
      // Mensaje inválido: reintentar no lo arregla, va directo a la DLQ.
      await this.sendToDlq(meta, value, err as Error, 0);
      await commit();
      return 'dlq';
    }

    let lastError: Error = new Error('desconocido');
    for (let attempt = 0; attempt <= this.retryDelays.length; attempt++) {
      try {
        const outcome = await this.runOnce(event, meta, handler);
        await commit(); // recién ahora: nunca antes de procesar
        if (outcome === 'duplicate') {
          this.log.warn(`[${event.correlationId}] ${event.eventType} duplicado (${event.eventId}); se descarta`);
        }
        return outcome;
      } catch (err) {
        lastError = err as Error;
        if (attempt < this.retryDelays.length) {
          const wait = this.retryDelays[attempt];
          this.log.warn(
            `[${event.correlationId}] ${event.eventType} falló (${lastError.message}); reintento ${attempt + 1}/${this.retryDelays.length} en ${wait} ms`,
          );
          await sleep(wait);
        }
      }
    }

    await this.sendToDlq(meta, value, lastError, this.retryDelays.length + 1);
    await commit(); // la partición sigue con el próximo mensaje
    return 'dlq';
  }

  private async runOnce(event: DomainEvent, meta: MessageMeta, handler: EventHandler): Promise<'processed' | 'duplicate'> {
    const client = await this.pool.connect();
    const after: Array<() => void | Promise<void>> = [];
    try {
      await client.query('BEGIN');
      if (!(await markProcessed(client, event.eventId, this.options.consumer))) {
        await client.query('ROLLBACK');
        return 'duplicate';
      }
      await handler(event, {
        client,
        meta,
        enqueue: (e) => enqueueEvent(client, e),
        afterCommit: (fn) => void after.push(fn),
      });
      await client.query('COMMIT');
    } catch (err) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw err;
    } finally {
      client.release();
    }
    for (const fn of after) {
      try {
        await fn();
      } catch (err) {
        this.log.warn(`afterCommit falló: ${(err as Error).message}`);
      }
    }
    return 'processed';
  }

  private async sendToDlq(meta: MessageMeta, original: unknown, error: Error, attempts: number): Promise<void> {
    const dlq = dlqOf(meta.topic);
    const key = meta.key ?? (original && typeof original === 'object' ? partitionKey(original as { customerId: string }) : '') ?? '';
    await this.publisher.publish({
      topic: dlq,
      key: key || 'unknown',
      value: original, // se reenvía el mensaje original, intacto
      headers: {
        'x-error-reason': error.message.slice(0, 500),
        'x-original-topic': meta.topic,
        'x-original-partition': String(meta.partition),
        'x-original-offset': meta.offset,
        'x-failed-consumer': this.options.consumer,
        'x-attempts': String(attempts),
        'x-failed-at': new Date().toISOString(),
      },
    });
    this.log.error(`Mensaje de ${meta.topic}[${meta.partition}]@${meta.offset} enviado a ${dlq}: ${error.message}`);
  }
}
