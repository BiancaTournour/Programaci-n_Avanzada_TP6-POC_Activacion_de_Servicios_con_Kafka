import { Injectable, Logger } from '@nestjs/common';
import { createEvent, SERVICES, type DomainEvent } from '@poc/contracts';
import { envInt, lockKey, sleep, type HandlerContext } from '@poc/kafka-toolkit';

const rand = (min: number, max: number) => min + Math.floor(Math.random() * (max - min + 1));

@Injectable()
export class BillingService {
  private readonly log = new Logger(BillingService.name);
  private readonly minDelay = envInt('BILLING_DELAY_MIN_MS', 500);
  private readonly maxDelay = envInt('BILLING_DELAY_MAX_MS', 1500);

  /** RF-04: crea la cuenta de facturación simulada (o falla si simulateFailure = billing). */
  async onActivationRequested(event: DomainEvent, ctx: HandlerContext): Promise<void> {
    if (event.eventType !== 'ActivationRequested') return;
    const { client } = ctx;
    const id = event.correlationId;
    await lockKey(client, id); // serializa con onActivationEvent de la misma activación
    await sleep(rand(this.minDelay, this.maxDelay)); // latencia simulada

    if (event.payload.simulateFailure === 'billing') {
      // Error de NEGOCIO simulado: se publica BillingFailed, no va a la DLQ.
      const reason = 'Fallo simulado: tarjeta rechazada';
      await client.query(
        `INSERT INTO billing_accounts (activation_id, customer_id, status, reason) VALUES ($1, $2, 'FAILED', $3)
         ON CONFLICT (activation_id) DO NOTHING`,
        [id, event.customerId, reason],
      );
      await ctx.enqueue(
        createEvent('BillingFailed', { correlationId: id, customerId: event.customerId, source: SERVICES.BILLING, payload: { reason } }),
      );
      this.log.log(`[${id}] BillingFailed (simulado)`);
      return;
    }

    const accountId = `BA-${id}`;
    const ins = await client.query(
      `INSERT INTO billing_accounts (activation_id, customer_id, account_id, status) VALUES ($1, $2, $3, 'CREATED')
       ON CONFLICT (activation_id) DO NOTHING`,
      [id, event.customerId, accountId],
    );
    if (!ins.rowCount) {
      this.log.warn(`[${id}] la cuenta ya existía; no se crea otra`);
      return;
    }
    await ctx.enqueue(
      createEvent('BillingAccountCreated', { correlationId: id, customerId: event.customerId, source: SERVICES.BILLING, payload: { accountId } }),
    );
    this.log.log(`[${id}] cuenta ${accountId} creada`);

    // Caso borde: ActivationFailed se vio antes de crear la cuenta => se anula igual.
    const seen = await client.query('SELECT 1 FROM activation_failures WHERE activation_id = $1', [id]);
    if (seen.rowCount) {
      await this.cancel(event, accountId, ctx, 'La activación ya había fallado antes de crear la cuenta');
    }
  }

  /** RF-06 (compensación): ante ActivationFailed anula la cuenta si existe. */
  async onActivationEvent(event: DomainEvent, ctx: HandlerContext): Promise<void> {
    if (event.eventType !== 'ActivationFailed') return; // ActivationCompleted no requiere acción
    const { client } = ctx;
    const id = event.correlationId;
    await lockKey(client, id);

    const { rows } = await client.query('SELECT account_id, status FROM billing_accounts WHERE activation_id = $1 FOR UPDATE', [id]);
    if (!rows[0]) {
      // Todavía no procesé ActivationRequested: lo dejo anotado para anular al crearla.
      await client.query('INSERT INTO activation_failures (activation_id) VALUES ($1) ON CONFLICT DO NOTHING', [id]);
      this.log.warn(`[${id}] ActivationFailed antes de crear la cuenta; se anulará al crearla`);
      return;
    }
    if (rows[0].status === 'CREATED') {
      await this.cancel(event, rows[0].account_id, ctx, `Compensación: ${event.payload.reason}`);
    }
    // FAILED o CANCELLED: no hay nada que anular (idempotente).
  }

  private async cancel(event: DomainEvent, accountId: string, ctx: HandlerContext, reason: string): Promise<void> {
    await ctx.client.query(`UPDATE billing_accounts SET status = 'CANCELLED', reason = $2, cancelled_at = now() WHERE activation_id = $1`, [
      event.correlationId,
      reason,
    ]);
    await ctx.enqueue(
      createEvent('BillingAccountCancelled', {
        correlationId: event.correlationId,
        customerId: event.customerId,
        source: SERVICES.BILLING,
        payload: { accountId, reason },
      }),
    );
    this.log.log(`[${event.correlationId}] cuenta ${accountId} ANULADA (compensación)`);
  }
}
