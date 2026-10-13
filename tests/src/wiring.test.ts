/**
 * Smoke test del cableado de NestJS (DI, controladores, HTTP, handle() con KafkaContext).
 * Solo se reemplaza el productor de Kafka por uno en memoria; Postgres es real.
 */
import 'reflect-metadata';
import { test, after } from 'node:test';
import assert from 'node:assert/strict';
import { Test } from '@nestjs/testing';
import type { INestApplication } from '@nestjs/common';
import { createEvent, type DomainEvent } from '@poc/contracts';
import { EVENT_PUBLISHER, type OutgoingMessage } from '@poc/kafka-toolkit';

const PG = process.env.TEST_PG_URL;
const opts = { skip: PG ? false : 'definí TEST_PG_URL' };

const sent: OutgoingMessage[] = [];
const fakePublisher = { publish: async (m: OutgoingMessage) => void sent.push(m) };
const apps: INestApplication[] = [];
after(async () => { for (const a of apps) await a.close(); });

const kafkaCtx = (topic: string, offset: string, key: string) => {
  const committed: unknown[] = [];
  const ctx = {
    getMessage: () => ({ offset, key: Buffer.from(key) }),
    getTopic: () => topic,
    getPartition: () => 1,
    getConsumer: () => ({ commitOffsets: async (o: unknown) => void committed.push(o) }),
  };
  return { ctx: ctx as any, committed };
};
const until = async (fn: () => boolean, ms = 4000) => {
  const t = Date.now();
  while (!fn()) { if (Date.now() - t > ms) throw new Error('timeout'); await new Promise((r) => setTimeout(r, 50)); }
};

test('activation-api: HTTP 202, outbox -> Kafka, y el SagaController agrega resultados', opts, async () => {
  process.env.DATABASE_URL = `${PG}/wiring_test_act`;
  const { AppModule } = require('@poc/activation-api/dist/app.module');
  const { SagaController } = require('@poc/activation-api/dist/saga.controller');
  const moduleRef = await Test.createTestingModule({ imports: [AppModule] })
    .overrideProvider(EVENT_PUBLISHER).useValue(fakePublisher).compile();
  const app = moduleRef.createNestApplication();
  apps.push(app);
  await app.init();
  await app.listen(0);
  const base = await app.getUrl();
  await (app.get('PG_POOL') as any).query('TRUNCATE activations, processed_events, outbox');
  await (app.get('PG_POOL') as any).query('ALTER SEQUENCE activation_seq RESTART');

  // RF-02
  const res = await fetch(`${base}/activations`, {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ customerId: 'C-1234', planId: 'FLOW-FULL', simulateFailure: 'none' }),
  });
  assert.equal(res.status, 202);
  assert.deepEqual(await res.json(), { activationId: 'act-0001', status: 'PENDING' });
  assert.equal((await fetch(`${base}/activations`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: '{}' })).status, 400);
  assert.equal((await fetch(`${base}/activations/act-9999`)).status, 404);

  // RF-03: el relay del outbox publica ActivationRequested con customerId como clave
  await until(() => sent.some((m) => m.topic === 'activation.requested'));
  const req = sent.find((m) => m.topic === 'activation.requested')!;
  assert.equal(req.key, 'C-1234');
  assert.equal((req.value as DomainEvent).correlationId, 'act-0001');

  // El consumer de la saga (con KafkaContext simulado) agrega los resultados y confirma el offset DESPUÉS
  const saga = app.get(SagaController);
  const mk = (t: 'BillingAccountCreated' | 'ProvisioningCompleted', payload: any) =>
    createEvent(t, { correlationId: 'act-0001', customerId: 'C-1234', source: 'x', payload });
  const a = kafkaCtx('billing.events', '41', 'C-1234');
  assert.equal(await saga.onBilling(mk('BillingAccountCreated', { accountId: 'BA-1' }), a.ctx), 'processed');
  assert.deepEqual(a.committed, [[{ topic: 'billing.events', partition: 1, offset: '42' }]], 'commit del offset + 1');
  assert.equal((await (await fetch(`${base}/activations/act-0001`)).json() as any).status, 'IN_PROGRESS');
  const b = kafkaCtx('provisioning.events', '7', 'C-1234');
  await saga.onProvisioning(mk('ProvisioningCompleted', { orderId: 'PO-1' }), b.ctx);
  const final = await (await fetch(`${base}/activations/act-0001`)).json() as any;
  assert.equal(final.status, 'ACTIVE');
  assert.equal(final.history.length, 4);
  await until(() => sent.some((m) => (m.value as DomainEvent).eventType === 'ActivationCompleted'));
  assert.equal(sent.find((m) => (m.value as DomainEvent).eventType === 'ActivationCompleted')!.topic, 'activation.events');
  assert.equal((await (await fetch(`${base}/activations`)).json() as any[]).length, 1);
});

test('billing-service: el BillingController consume ActivationRequested y publica BillingAccountCreated', opts, async () => {
  process.env.DATABASE_URL = `${PG}/wiring_test_billing`;
  process.env.BILLING_DELAY_MIN_MS = '0'; process.env.BILLING_DELAY_MAX_MS = '0';
  const { AppModule } = require('@poc/billing-service/dist/app.module');
  const { BillingController } = require('@poc/billing-service/dist/billing.controller');
  const moduleRef = await Test.createTestingModule({ imports: [AppModule] })
    .overrideProvider(EVENT_PUBLISHER).useValue(fakePublisher).compile();
  const app = moduleRef.createNestApplication();
  apps.push(app);
  await app.init();
  await (app.get('PG_POOL') as any).query('TRUNCATE billing_accounts, processed_events, outbox');
  const ev = createEvent('ActivationRequested', {
    correlationId: 'act-0042', customerId: 'C-7', source: 'activation-api',
    payload: { planId: 'FLOW-FULL', channel: 'web', simulateFailure: 'none' },
  });
  const { ctx, committed } = kafkaCtx('activation.requested', '0', 'C-7');
  assert.equal(await app.get(BillingController).onRequested(ev, ctx), 'processed');
  assert.equal(committed.length, 1);
  await until(() => sent.some((m) => (m.value as DomainEvent)?.correlationId === 'act-0042' && (m.value as DomainEvent).eventType === 'BillingAccountCreated'));
  assert.equal(sent.find((m) => (m.value as DomainEvent)?.eventType === 'BillingAccountCreated')!.topic, 'billing.events');
  // Mismo evento otra vez => duplicado, no se crea otra cuenta
  assert.equal(await app.get(BillingController).onRequested(ev, kafkaCtx('activation.requested', '0', 'C-7').ctx), 'duplicate');
});
