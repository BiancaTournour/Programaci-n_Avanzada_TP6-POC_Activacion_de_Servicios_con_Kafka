/**
 * Prueba de la saga completa con los servicios REALES (activation-api, billing, provisioning,
 * notification, crm-analytics, loyalty) y un PostgreSQL real. Lo único simulado es el broker:
 * un bus en memoria hace de Kafka (entrega cada mensaje a los consumidores suscriptos al topic).
 *
 *   TEST_PG_URL=postgres://poc:poc@localhost:5432 pnpm --filter @poc/tests test
 */
import 'reflect-metadata';
process.env.BILLING_DELAY_MIN_MS = '0';
process.env.BILLING_DELAY_MAX_MS = '0';
process.env.PROVISIONING_DELAY_MIN_MS = '0';
process.env.PROVISIONING_DELAY_MAX_MS = '0';

import { test, before, after, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { TOPICS, type DomainEvent } from '@poc/contracts';
import {
  EventProcessor, OutboxRelay, createPool, ensureDatabase, initSchema,
  type EventHandler, type EventPublisher, type OutgoingMessage,
} from '@poc/kafka-toolkit';
import type { Pool } from 'pg';
import { ActivationsService } from '@poc/activation-api/dist/activations.service';
import { ACTIVATION_SCHEMA } from '@poc/activation-api/dist/schema';
import type { ActivationView } from '@poc/activation-api/dist/activation.types';
import { BillingService } from '@poc/billing-service/dist/billing.service';
import { BILLING_SCHEMA } from '@poc/billing-service/dist/schema';
import { ProvisioningService } from '@poc/provisioning-service/dist/provisioning.service';
import { PROVISIONING_SCHEMA } from '@poc/provisioning-service/dist/schema';
import { NotificationService } from '@poc/notification-service/dist/notification.service';
import { NOTIFICATION_SCHEMA } from '@poc/notification-service/dist/schema';
import { Mailer, type MailMessage } from '@poc/notification-service/dist/mailer';
import { CrmService } from '@poc/crm-analytics-service/dist/crm.service';
import { CRM_SCHEMA } from '@poc/crm-analytics-service/dist/schema';
import { LoyaltyService } from '@poc/loyalty-service/dist/loyalty.service';
import { LOYALTY_SCHEMA } from '@poc/loyalty-service/dist/schema';

const PG = process.env.TEST_PG_URL;
const opts = { skip: PG ? false : 'definí TEST_PG_URL (ej. postgres://poc:poc@localhost:5432)' };

// ---------------- Bus en memoria (hace de Kafka) ----------------
interface Sub { service: string; topic: string; processor: EventProcessor; handler: EventHandler; paused: boolean; held: LogEntry[] }
interface LogEntry { msg: OutgoingMessage; offset: number }

class Bus implements EventPublisher {
  log: LogEntry[] = [];
  private queue: LogEntry[] = [];
  subs: Sub[] = [];
  async publish(msg: OutgoingMessage) {
    const entry = { msg, offset: this.log.length };
    this.log.push(entry);
    this.queue.push(entry);
  }
  subscribe(service: string, topic: string, processor: EventProcessor, handler: EventHandler) {
    this.subs.push({ service, topic, processor, handler, paused: false, held: [] });
  }
  pause(service: string, topic: string) { this.sub(service, topic).paused = true; }
  async resume(service: string, topic: string) {
    const s = this.sub(service, topic);
    s.paused = false;
    for (const e of s.held.splice(0)) await this.deliver(s, e);
  }
  private sub(service: string, topic: string) { return this.subs.find((s) => s.service === service && s.topic === topic)!; }
  async deliver(s: Sub, e: LogEntry) {
    await s.processor.processMessage(
      { topic: e.msg.topic, partition: 0, offset: String(e.offset), key: e.msg.key }, e.msg.value, s.handler, async () => {},
    );
  }
  async drain() {
    while (this.queue.length) {
      const e = this.queue.shift()!;
      for (const s of this.subs.filter((x) => x.topic === e.msg.topic)) {
        if (s.paused) s.held.push(e); else await this.deliver(s, e);
      }
    }
  }
  /** Reentrega TODO el log (simula "al menos una vez": Kafka entrega de nuevo). */
  async redeliverAll() {
    for (const e of [...this.log]) for (const s of this.subs.filter((x) => x.topic === e.msg.topic)) await this.deliver(s, e);
  }
  eventTypes(correlationId: string): string[] {
    return this.log.map((e) => e.msg.value as DomainEvent).filter((v) => v?.correlationId === correlationId).map((v) => v.eventType);
  }
}

class FakeMailer extends Mailer {
  sent: MailMessage[] = [];
  async send(m: MailMessage) { this.sent.push(m); }
}

// ---------------- Armado del sistema ----------------
interface World {
  bus: Bus; pools: Record<string, Pool>; relays: OutboxRelay[]; mailer: FakeMailer;
  ws: ActivationView[]; activations: ActivationsService;
  settle(): Promise<void>;
}
let w: World;
const q = async (db: string, sql: string, params: unknown[] = []) => (await w.pools[db].query(sql, params)).rows;
const DBS = ['act', 'billing', 'prov', 'notif', 'crm', 'loyalty'];

async function buildWorld(): Promise<World> {
  const bus = new Bus();
  const pools: Record<string, Pool> = {};
  const schemas: Record<string, { schema: string[]; outbox: boolean }> = {
    act: { schema: ACTIVATION_SCHEMA, outbox: true }, billing: { schema: BILLING_SCHEMA, outbox: true },
    prov: { schema: PROVISIONING_SCHEMA, outbox: true }, notif: { schema: NOTIFICATION_SCHEMA, outbox: false },
    crm: { schema: CRM_SCHEMA, outbox: false }, loyalty: { schema: LOYALTY_SCHEMA, outbox: false },
  };
  for (const db of DBS) {
    const url = `${PG}/saga_test_${db}`;
    await ensureDatabase(url);
    pools[db] = createPool(url);
    await initSchema(pools[db], { outbox: schemas[db].outbox, extra: schemas[db].schema });
    await pools[db].query(`TRUNCATE processed_events${schemas[db].outbox ? ', outbox' : ''} CASCADE`);
  }
  await pools.act.query('TRUNCATE activations');
  await pools.billing.query('TRUNCATE billing_accounts, activation_failures');
  await pools.prov.query('TRUNCATE provisioning_orders');
  await pools.notif.query('TRUNCATE notifications');
  await pools.crm.query('TRUNCATE event_log');
  await pools.loyalty.query('TRUNCATE loyalty_points');
  await pools.act.query('ALTER SEQUENCE activation_seq RESTART');

  const mailer = new FakeMailer();
  const ws: ActivationView[] = [];
  const proc = (db: string, group: string) => new EventProcessor(pools[db], bus, { consumer: group, retryDelaysMs: [1, 1, 1] });
  const activations = new ActivationsService(pools.act, { publish: (a) => void ws.push(a) });
  const billing = new BillingService();
  const prov = new ProvisioningService();
  const notif = new NotificationService(mailer);
  const crm = new CrmService();
  const loyalty = new LoyaltyService();

  const pa = proc('act', 'activation-api'), pb = proc('billing', 'billing-svc'), pp = proc('prov', 'provisioning-svc');
  const pn = proc('notif', 'notification-svc'), pc = proc('crm', 'crm-analytics'), pl = proc('loyalty', 'loyalty-svc');
  bus.subscribe('act', TOPICS.BILLING_EVENTS, pa, (e, c) => activations.applyResult(e, c));
  bus.subscribe('act', TOPICS.PROVISIONING_EVENTS, pa, (e, c) => activations.applyResult(e, c));
  bus.subscribe('billing', TOPICS.ACTIVATION_REQUESTED, pb, (e, c) => billing.onActivationRequested(e, c));
  bus.subscribe('billing', TOPICS.ACTIVATION_EVENTS, pb, (e, c) => billing.onActivationEvent(e, c));
  bus.subscribe('prov', TOPICS.ACTIVATION_REQUESTED, pp, (e, c) => prov.onActivationRequested(e, c));
  bus.subscribe('notif', TOPICS.ACTIVATION_EVENTS, pn, (e, c) => notif.onActivationEvent(e, c));
  for (const t of Object.values(TOPICS)) bus.subscribe('crm', t, pc, (e, c) => crm.record(e, c));
  bus.subscribe('loyalty', TOPICS.ACTIVATION_EVENTS, pl, (e, c) => loyalty.onActivationEvent(e, c));

  const relays = [pools.act, pools.billing, pools.prov].map((p) => new OutboxRelay(p, bus));
  const settle = async () => {
    for (let i = 0; i < 20; i++) {
      let moved = 0;
      for (const r of relays) moved += await r.tick();
      await bus.drain();
      if (!moved) return;
    }
    throw new Error('la saga no se estabiliza');
  };
  return { bus, pools, relays, mailer, ws, activations, settle };
}

before(async () => { if (PG) w = await buildWorld(); });
beforeEach(async () => {
  if (!PG) return;
  for (const p of Object.values(w.pools)) await p.end();
  w = await buildWorld();
});
after(async () => { if (PG) for (const p of Object.values(w.pools)) await p.end(); });

const create = (simulateFailure = 'none', customerId = 'C-1234') =>
  w.activations.create({ customerId, planId: 'FLOW-FULL', simulateFailure });

// ---------------- Escenarios ----------------
test('RF-02: crear responde PENDING con activationId, sin esperar a nadie', opts, async () => {
  const r = await create();
  assert.equal(r.activationId, 'act-0001');
  assert.equal(r.status, 'PENDING');
  assert.equal((await w.activations.get(r.activationId)).status, 'PENDING');
  assert.equal(w.bus.log.length, 0, 'todavía no se publicó nada: lo hace el relay del outbox');
});

test('Escenario 1 – camino feliz: fan-out, ACTIVE, un email, CRM y loyalty reaccionan', opts, async () => {
  const { activationId } = await create();
  await w.settle();
  const act = await w.activations.get(activationId);
  assert.equal(act.status, 'ACTIVE');
  assert.equal(act.steps.billing?.status, 'OK');
  assert.equal(act.steps.provisioning?.status, 'OK');
  const types = act.history.map((h) => h.eventType);
  assert.deepEqual(types.slice().sort(), ['ActivationCompleted', 'ActivationRequested', 'BillingAccountCreated', 'ProvisioningCompleted']);
  assert.equal(types[0], 'ActivationRequested');
  assert.equal(types.at(-1), 'ActivationCompleted');
  assert.equal(w.mailer.sent.length, 1);
  assert.match(w.mailer.sent[0].subject, /ya está activo/);
  assert.equal((await q('billing', 'SELECT status FROM billing_accounts'))[0].status, 'CREATED');
  assert.equal((await q('crm', 'SELECT count(*)::int n FROM event_log WHERE correlation_id=$1', [activationId]))[0].n, 4);
  assert.equal((await q('loyalty', 'SELECT points FROM loyalty_points'))[0].points, 100);
  // RF-08: la UI recibe un mensaje por cada cambio de estado; el último ya trae el timeline completo.
  assert.equal(w.ws.length, 3, 'creación + resultado de billing + resultado de provisioning');
  assert.equal(w.ws.at(-1)!.status, 'ACTIVE');
  assert.equal(w.ws.at(-1)!.history.length, 4);
});

test('Escenario 2 – fallo en provisioning: compensación sin llamadas directas', opts, async () => {
  const { activationId } = await create('provisioning');
  await w.settle();
  const act = await w.activations.get(activationId);
  assert.equal(act.status, 'FAILED');
  const types = w.bus.eventTypes(activationId);
  for (const t of ['ProvisioningFailed', 'ActivationFailed', 'BillingAccountCancelled']) assert.ok(types.includes(t), `falta ${t}`);
  assert.ok(!types.includes('ActivationCompleted'));
  assert.equal((await q('billing', 'SELECT status FROM billing_accounts'))[0].status, 'CANCELLED');
  assert.equal(w.mailer.sent.length, 1);
  assert.match(w.mailer.sent[0].subject, /No pudimos activar/);
  assert.equal((await q('loyalty', 'SELECT count(*)::int n FROM loyalty_points'))[0].n, 0);
  assert.ok(act.steps.billing?.compensated);
});

test('fallo en billing: FAILED; el resultado tardío de provisioning solo se registra', opts, async () => {
  const { activationId } = await create('billing');
  await w.settle();
  const act = await w.activations.get(activationId);
  assert.equal(act.status, 'FAILED');
  const types = act.history.map((h) => h.eventType);
  assert.ok(types.includes('ProvisioningCompleted'), 'se registra en el historial');
  assert.equal(types.filter((t) => t === 'ActivationFailed').length, 1, 'ActivationFailed se publica una sola vez');
  assert.ok(!types.includes('ActivationCompleted'));
  assert.equal((await q('billing', 'SELECT status FROM billing_accounts'))[0].status, 'FAILED');
  assert.ok(!types.includes('BillingAccountCancelled'), 'no había cuenta que anular');
  assert.equal(w.mailer.sent.length, 1);
});

test('caso borde: ActivationFailed llega a billing ANTES de crear la cuenta -> se anula igual', opts, async () => {
  w.bus.pause('billing', TOPICS.ACTIVATION_REQUESTED); // billing aún no vio ActivationRequested
  const { activationId } = await create('provisioning');
  await w.settle();
  assert.equal((await w.activations.get(activationId)).status, 'FAILED');
  assert.equal((await q('billing', 'SELECT count(*)::int n FROM billing_accounts'))[0].n, 0);
  await w.bus.resume('billing', TOPICS.ACTIVATION_REQUESTED);
  await w.settle();
  assert.equal((await q('billing', 'SELECT status FROM billing_accounts'))[0].status, 'CANCELLED');
  assert.ok(w.bus.eventTypes(activationId).includes('BillingAccountCancelled'));
});

test('RNF-03 / criterio de aceptación: reenviar todos los eventos no duplica cuentas ni emails', opts, async () => {
  const ok = await create();
  const bad = await create('provisioning', 'C-9999');
  await w.settle();
  const snapshot = async () => ({
    accounts: (await q('billing', 'SELECT count(*)::int n FROM billing_accounts'))[0].n,
    orders: (await q('prov', 'SELECT count(*)::int n FROM provisioning_orders'))[0].n,
    emails: w.mailer.sent.length,
    notifications: (await q('notif', 'SELECT count(*)::int n FROM notifications'))[0].n,
    crm: (await q('crm', 'SELECT count(*)::int n FROM event_log'))[0].n,
    points: (await q('loyalty', 'SELECT sum(points)::int n FROM loyalty_points'))[0].n,
    outbox: (await q('billing', 'SELECT count(*)::int n FROM outbox'))[0].n,
    hist: [(await w.activations.get(ok.activationId)).history.length, (await w.activations.get(bad.activationId)).history.length],
  });
  const before = await snapshot();
  await w.bus.redeliverAll();
  await w.settle();
  assert.deepEqual(await snapshot(), before);
  assert.equal(before.emails, 2);
});

test('RNF-02: todos los mensajes de un cliente usan customerId como clave', opts, async () => {
  await create('none', 'C-AAA');
  await create('provisioning', 'C-BBB');
  await w.settle();
  for (const e of w.bus.log) assert.equal(e.msg.key, (e.msg.value as DomainEvent).customerId);
});

test('RNF-06: todos los eventos de una activación comparten correlationId', opts, async () => {
  const { activationId } = await create();
  await w.settle();
  const ids = (await q('crm', 'SELECT DISTINCT correlation_id FROM event_log')).map((r) => r.correlation_id);
  assert.deepEqual(ids, [activationId]);
});

test('criterio de aceptación: un mensaje inválido va a la DLQ y el resto del flujo sigue', opts, async () => {
  const billingSub = w.bus.subs.find((s) => s.service === 'billing' && s.topic === TOPICS.ACTIVATION_REQUESTED)!;
  await w.bus.deliver(billingSub, { msg: { topic: TOPICS.ACTIVATION_REQUESTED, key: 'C-X', value: '{no es un evento' }, offset: 999 });
  const dlq = w.bus.log.find((e) => e.msg.topic === 'activation.requested.dlq');
  assert.ok(dlq, 'el mensaje roto terminó en la DLQ');
  assert.equal(dlq!.msg.value, '{no es un evento');
  const { activationId } = await create();
  await w.settle();
  assert.equal((await w.activations.get(activationId)).status, 'ACTIVE');
});

test('validación de entrada', opts, async () => {
  await assert.rejects(() => w.activations.create({ planId: 'X' }), /customerId/);
  await assert.rejects(() => w.activations.create({ customerId: 'C', planId: 'X', simulateFailure: 'otro' }), /simulateFailure/);
});
