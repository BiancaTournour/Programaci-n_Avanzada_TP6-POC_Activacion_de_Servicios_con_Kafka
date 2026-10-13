import { BadRequestException, Inject, Injectable, Logger, NotFoundException } from '@nestjs/common';
import {
  createEvent,
  SERVICES,
  type ActivationStatus,
  type DomainEvent,
  type FailureMode,
} from '@poc/contracts';
import { PG_POOL, enqueueEvent, withTransaction, type HandlerContext } from '@poc/kafka-toolkit';
import type { Pool } from 'pg';
import { ACTIVATION_NOTIFIER, type ActivationNotifier, type ActivationView, type HistoryEntry } from './activation.types';

export interface CreateActivationInput {
  customerId?: unknown;
  planId?: unknown;
  simulateFailure?: unknown;
  channel?: unknown;
}

const FAILURE_MODES: FailureMode[] = ['none', 'billing', 'provisioning'];

const toIso = (v: Date | string): string => (v instanceof Date ? v.toISOString() : v);

function toView(row: Record<string, any>): ActivationView {
  return {
    id: row.id,
    customerId: row.customer_id,
    planId: row.plan_id,
    channel: row.channel,
    status: row.status,
    simulateFailure: row.simulate_failure,
    steps: row.steps,
    history: row.history,
    createdAt: toIso(row.created_at),
    updatedAt: toIso(row.updated_at),
  };
}

@Injectable()
export class ActivationsService {
  private readonly log = new Logger(ActivationsService.name);

  constructor(
    @Inject(PG_POOL) private readonly pool: Pool,
    @Inject(ACTIVATION_NOTIFIER) private readonly notifier: ActivationNotifier,
  ) {}

  /**
   * RF-01/02/03: guarda la activación (PENDING) y el evento ActivationRequested en el outbox,
   * en una sola transacción. No espera a ningún otro servicio.
   */
  async create(input: CreateActivationInput): Promise<{ activationId: string; status: ActivationStatus }> {
    const customerId = typeof input.customerId === 'string' ? input.customerId.trim() : '';
    const planId = typeof input.planId === 'string' ? input.planId.trim() : '';
    if (!customerId) throw new BadRequestException('customerId es obligatorio');
    if (!planId) throw new BadRequestException('planId es obligatorio');
    const simulateFailure = (input.simulateFailure ?? 'none') as FailureMode;
    if (!FAILURE_MODES.includes(simulateFailure)) {
      throw new BadRequestException(`simulateFailure debe ser uno de: ${FAILURE_MODES.join(', ')}`);
    }
    const channel = typeof input.channel === 'string' && input.channel ? input.channel : 'web';

    const view = await withTransaction(this.pool, async (client) => {
      const seq = await client.query(`SELECT 'act-' || lpad(nextval('activation_seq')::text, 4, '0') AS id`);
      const id: string = seq.rows[0].id;
      const event = createEvent('ActivationRequested', {
        correlationId: id,
        customerId,
        source: SERVICES.ACTIVATION_API,
        payload: { planId, channel, simulateFailure },
      });
      const history: HistoryEntry[] = [entry(event, `plan ${planId}`)];
      const ins = await client.query(
        `INSERT INTO activations (id, customer_id, plan_id, channel, status, simulate_failure, steps, history)
         VALUES ($1, $2, $3, $4, 'PENDING', $5, '{}'::jsonb, $6::jsonb) RETURNING *`,
        [id, customerId, planId, channel, simulateFailure, JSON.stringify(history)],
      );
      await enqueueEvent(client, event);
      return toView(ins.rows[0]);
    });

    this.log.log(`[${view.id}] activación creada (${customerId}, ${planId}, fallo=${simulateFailure})`);
    this.notifier.publish(view);
    return { activationId: view.id, status: view.status };
  }

  async get(id: string): Promise<ActivationView> {
    const { rows } = await this.pool.query('SELECT * FROM activations WHERE id = $1', [id]);
    if (!rows[0]) throw new NotFoundException(`Activación ${id} no encontrada`);
    return toView(rows[0]);
  }

  async list(limit = 20): Promise<ActivationView[]> {
    const { rows } = await this.pool.query('SELECT * FROM activations ORDER BY created_at DESC, id DESC LIMIT $1', [
      Math.min(Math.max(limit, 1), 100),
    ]);
    return rows.map(toView);
  }

  /**
   * Agregador de la saga (sección 06). Corre dentro de la transacción de EventProcessor.
   *  - Guarda el resultado de cada servicio en `steps` y el evento en `history`.
   *  - Billing OK + Provisioning OK  -> ACTIVE + ActivationCompleted.
   *  - Primer fallo                   -> FAILED + ActivationFailed.
   *  - Resultados posteriores a un estado final solo se registran en el historial.
   */
  async applyResult(event: DomainEvent, ctx: HandlerContext): Promise<void> {
    const { rows } = await ctx.client.query('SELECT * FROM activations WHERE id = $1 FOR UPDATE', [event.correlationId]);
    if (!rows[0]) throw new Error(`Activación desconocida: ${event.correlationId}`);
    const act = toView(rows[0]);

    const steps = { ...act.steps };
    const history = [...act.history];
    let status = act.status;
    let outgoing: DomainEvent | undefined;
    const final = status === 'ACTIVE' || status === 'FAILED';

    switch (event.eventType) {
      case 'BillingAccountCancelled': {
        // Confirmación de la compensación: solo se registra.
        steps.billing = { status: steps.billing?.status ?? 'OK', at: steps.billing?.at ?? event.occurredAt, compensated: true };
        history.push(entry(event, `cuenta ${event.payload.accountId} anulada`));
        break;
      }
      case 'BillingAccountCreated':
      case 'BillingFailed':
      case 'ProvisioningCompleted':
      case 'ProvisioningFailed': {
        const step = event.eventType.startsWith('Billing') ? 'billing' : 'provisioning';
        const ok = event.eventType === 'BillingAccountCreated' || event.eventType === 'ProvisioningCompleted';
        const reason = 'reason' in event.payload ? (event.payload as { reason: string }).reason : undefined;
        steps[step] = { status: ok ? 'OK' : 'FAILED', at: event.occurredAt, ...(reason ? { reason } : {}) };
        history.push(entry(event, reason));

        if (!final) {
          if (!ok) {
            status = 'FAILED';
            outgoing = createEvent('ActivationFailed', {
              correlationId: act.id,
              customerId: act.customerId,
              source: SERVICES.ACTIVATION_API,
              payload: { failedStep: step, reason: reason ?? 'Fallo en ' + step },
            });
          } else if (steps.billing?.status === 'OK' && steps.provisioning?.status === 'OK') {
            status = 'ACTIVE';
            outgoing = createEvent('ActivationCompleted', {
              correlationId: act.id,
              customerId: act.customerId,
              source: SERVICES.ACTIVATION_API,
              payload: { planId: act.planId },
            });
          } else {
            status = 'IN_PROGRESS';
          }
        }
        break;
      }
      default:
        throw new Error(`activation-api no espera ${event.eventType} en este topic`);
    }

    if (outgoing) {
      history.push(entry(outgoing, outgoing.eventType === 'ActivationFailed' ? (outgoing.payload as { reason: string }).reason : undefined));
      await ctx.enqueue(outgoing);
    }

    const upd = await ctx.client.query(
      `UPDATE activations SET status = $2, steps = $3::jsonb, history = $4::jsonb, updated_at = now()
       WHERE id = $1 RETURNING *`,
      [act.id, status, JSON.stringify(steps), JSON.stringify(history)],
    );
    const view = toView(upd.rows[0]);
    this.log.log(`[${act.id}] ${event.eventType} -> ${status}`);
    ctx.afterCommit(() => this.notifier.publish(view));
  }
}

function entry(event: DomainEvent, detail?: string): HistoryEntry {
  return {
    eventType: event.eventType,
    at: event.occurredAt,
    eventId: event.eventId,
    source: event.source,
    ...(detail ? { detail } : {}),
  };
}
