import { Injectable, Logger } from '@nestjs/common';
import type { DomainEvent } from '@poc/contracts';
import type { HandlerContext } from '@poc/kafka-toolkit';

const POINTS_PER_ACTIVATION = 100;

/**
 * Escenario 5: servicio NUEVO que se suma sin tocar a los demás. Con auto.offset.reset=earliest
 * procesa todo el historial de activation.events y acumula puntos por cada activación exitosa.
 */
@Injectable()
export class LoyaltyService {
  private readonly log = new Logger(LoyaltyService.name);

  async onActivationEvent(event: DomainEvent, ctx: HandlerContext): Promise<void> {
    if (event.eventType !== 'ActivationCompleted') return;
    const r = await ctx.client.query(
      `INSERT INTO loyalty_points (customer_id, points, activations) VALUES ($1, $2, 1)
       ON CONFLICT (customer_id) DO UPDATE
         SET points = loyalty_points.points + $2, activations = loyalty_points.activations + 1, updated_at = now()
       RETURNING points`,
      [event.customerId, POINTS_PER_ACTIVATION],
    );
    this.log.log(`[${event.correlationId}] ${event.customerId} suma ${POINTS_PER_ACTIVATION} puntos (total ${r.rows[0].points})`);
  }
}
