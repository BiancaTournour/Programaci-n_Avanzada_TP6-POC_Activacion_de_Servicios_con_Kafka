import { Injectable, Logger } from '@nestjs/common';
import type { DomainEvent } from '@poc/contracts';
import type { HandlerContext } from '@poc/kafka-toolkit';
import { Mailer } from './mailer';

@Injectable()
export class NotificationService {
  private readonly log = new Logger(NotificationService.name);

  constructor(private readonly mailer: Mailer) {}

  /** RF-07: un email por cada ActivationCompleted o ActivationFailed. */
  async onActivationEvent(event: DomainEvent, ctx: HandlerContext): Promise<void> {
    if (event.eventType !== 'ActivationCompleted' && event.eventType !== 'ActivationFailed') return;

    const ok = event.eventType === 'ActivationCompleted';
    const to = `${event.customerId.toLowerCase()}@clientes.poc.local`;
    const subject = ok
      ? `¡Tu plan ${event.payload.planId} ya está activo!`
      : `No pudimos activar tu plan (${event.correlationId})`;
    const text = ok
      ? `Hola ${event.customerId}, tu plan ${event.payload.planId} fue activado correctamente. Activación: ${event.correlationId}.`
      : `Hola ${event.customerId}, tu activación ${event.correlationId} falló en ${event.payload.failedStep}: ${event.payload.reason}. No se te cobrará nada.`;
    const html = `<h2>${subject}</h2><p>${text}</p>`;

    // Unicidad por (activación, tipo): si ya se registró, no se vuelve a mandar.
    const ins = await ctx.client.query(
      `INSERT INTO notifications (activation_id, event_type, to_address, subject) VALUES ($1, $2, $3, $4)
       ON CONFLICT (activation_id, event_type) DO NOTHING`,
      [event.correlationId, event.eventType, to, subject],
    );
    if (!ins.rowCount) {
      this.log.warn(`[${event.correlationId}] ya se notificó ${event.eventType}; no se reenvía`);
      return;
    }
    // Si el envío falla se lanza el error: ROLLBACK (también del INSERT) y el procesador reintenta.
    await this.mailer.send({ to, subject, text, html });
    this.log.log(`[${event.correlationId}] email "${subject}" enviado a ${to}`);
  }
}
