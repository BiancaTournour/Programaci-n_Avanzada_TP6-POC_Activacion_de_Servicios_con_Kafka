import { Injectable, Logger } from '@nestjs/common';
import type { DomainEvent } from '@poc/contracts';
import type { HandlerContext } from '@poc/kafka-toolkit';

@Injectable()
export class CrmService {
  private readonly log = new Logger(CrmService.name);

  /** RF-10: registra TODOS los eventos. No publica nada: es un consumidor puro. */
  async record(event: DomainEvent, ctx: HandlerContext): Promise<void> {
    await ctx.client.query(
      `INSERT INTO event_log (event_id, correlation_id, customer_id, event_type, version, source, topic,
                              kafka_partition, kafka_offset, occurred_at, payload)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb)
       ON CONFLICT (event_id) DO NOTHING`,
      [
        event.eventId, event.correlationId, event.customerId, event.eventType, event.version, event.source,
        ctx.meta.topic, ctx.meta.partition, ctx.meta.offset, event.occurredAt, JSON.stringify(event.payload),
      ],
    );
    this.log.log(`[${event.correlationId}] ${event.eventType} <- ${ctx.meta.topic}[${ctx.meta.partition}]@${ctx.meta.offset}`);
  }
}
