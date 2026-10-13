import { Injectable, Logger } from '@nestjs/common';
import { createEvent, SERVICES, type DomainEvent } from '@poc/contracts';
import { envInt, sleep, type HandlerContext } from '@poc/kafka-toolkit';

const rand = (min: number, max: number) => min + Math.floor(Math.random() * (max - min + 1));

@Injectable()
export class ProvisioningService {
  private readonly log = new Logger(ProvisioningService.name);
  // El documento pide un retardo de 1 a 3 segundos.
  private readonly minDelay = envInt('PROVISIONING_DELAY_MIN_MS', 1000);
  private readonly maxDelay = envInt('PROVISIONING_DELAY_MAX_MS', 3000);

  async onActivationRequested(event: DomainEvent, ctx: HandlerContext): Promise<void> {
    if (event.eventType !== 'ActivationRequested') return;
    const id = event.correlationId;
    await sleep(rand(this.minDelay, this.maxDelay));

    if (event.payload.simulateFailure === 'provisioning') {
      const reason = 'Fallo simulado: sin capacidad en la red';
      await ctx.client.query(
        `INSERT INTO provisioning_orders (activation_id, customer_id, status, reason) VALUES ($1, $2, 'FAILED', $3)
         ON CONFLICT (activation_id) DO NOTHING`,
        [id, event.customerId, reason],
      );
      await ctx.enqueue(
        createEvent('ProvisioningFailed', { correlationId: id, customerId: event.customerId, source: SERVICES.PROVISIONING, payload: { reason } }),
      );
      this.log.log(`[${id}] ProvisioningFailed (simulado)`);
      return;
    }

    const orderId = `PO-${id}`;
    const ins = await ctx.client.query(
      `INSERT INTO provisioning_orders (activation_id, customer_id, order_id, status) VALUES ($1, $2, $3, 'COMPLETED')
       ON CONFLICT (activation_id) DO NOTHING`,
      [id, event.customerId, orderId],
    );
    if (!ins.rowCount) {
      this.log.warn(`[${id}] la orden ya existía; no se crea otra`);
      return;
    }
    await ctx.enqueue(
      createEvent('ProvisioningCompleted', { correlationId: id, customerId: event.customerId, source: SERVICES.PROVISIONING, payload: { orderId } }),
    );
    this.log.log(`[${id}] línea aprovisionada (${orderId})`);
  }
}
