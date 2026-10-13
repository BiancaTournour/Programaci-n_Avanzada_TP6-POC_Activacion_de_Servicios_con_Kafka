import { Global, Inject, Module, type DynamicModule, type OnApplicationShutdown, type Provider } from '@nestjs/common';
import type { Pool } from 'pg';
import { createPool, ensureDatabase, initSchema } from './database';
import { OutboxRelay } from './outbox';
import { EventProcessor } from './processor';
import { KafkaPublisher, type EventPublisher } from './publisher';

export const PG_POOL = 'PG_POOL';
export const EVENT_PUBLISHER = 'EVENT_PUBLISHER';
export const TOOLKIT_OPTIONS = 'TOOLKIT_OPTIONS';

export interface ToolkitOptions {
  /** Nombre del servicio; se usa como clientId del productor. */
  serviceName: string;
  /** Consumer group del servicio (se registra en processed_events). */
  groupId: string;
  databaseUrl: string;
  /** DDL propio del servicio (CREATE TABLE IF NOT EXISTS ...). */
  schema?: string[];
  /** Activa la tabla outbox y el relay (servicios que publican eventos). */
  outbox?: boolean;
  retryDelaysMs?: number[];
}

class PoolCloser implements OnApplicationShutdown {
  constructor(@Inject(PG_POOL) private readonly pool: Pool) {}
  async onApplicationShutdown(): Promise<void> {
    await this.pool.end();
  }
}

/**
 * Módulo global: cada servicio lo importa una vez y obtiene PG_POOL, EVENT_PUBLISHER,
 * EventProcessor y (opcional) el relay del outbox.
 */
@Global()
@Module({})
export class ToolkitModule {
  static forRoot(options: ToolkitOptions): DynamicModule {
    const providers: Provider[] = [
      { provide: TOOLKIT_OPTIONS, useValue: options },
      {
        provide: PG_POOL,
        useFactory: async (): Promise<Pool> => {
          await ensureDatabase(options.databaseUrl);
          const pool = createPool(options.databaseUrl);
          await initSchema(pool, { outbox: !!options.outbox, extra: options.schema });
          return pool;
        },
      },
      {
        provide: EVENT_PUBLISHER,
        useFactory: async (): Promise<EventPublisher> => {
          const publisher = new KafkaPublisher(options.serviceName);
          await publisher.connect();
          return publisher;
        },
      },
      {
        provide: EventProcessor,
        inject: [PG_POOL, EVENT_PUBLISHER],
        useFactory: (pool: Pool, publisher: EventPublisher) =>
          new EventProcessor(pool, publisher, { consumer: options.groupId, retryDelaysMs: options.retryDelaysMs }),
      },
      PoolCloser,
    ];
    if (options.outbox) {
      providers.push({
        provide: OutboxRelay,
        inject: [PG_POOL, EVENT_PUBLISHER],
        useFactory: (pool: Pool, publisher: EventPublisher) => new OutboxRelay(pool, publisher),
      });
    }
    return {
      module: ToolkitModule,
      providers,
      exports: [PG_POOL, EVENT_PUBLISHER, TOOLKIT_OPTIONS, EventProcessor],
    };
  }
}
