/**
 * Pruebas del toolkit contra un PostgreSQL real.
 *   TEST_DATABASE_URL=postgres://poc:poc@localhost:5432/toolkit_test pnpm --filter @poc/kafka-toolkit test
 * Sin esa variable las pruebas se omiten.
 */
import { test, before, after, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { createEvent, type DomainEvent } from '@poc/contracts';
import type { Pool } from 'pg';
import { createPool, ensureDatabase, initSchema } from '../database';
import { EventProcessor, type MessageMeta } from '../processor';
import { OutboxRelay, enqueueEvent } from '../outbox';
import type { EventPublisher, OutgoingMessage } from '../publisher';

const URL = process.env.TEST_DATABASE_URL;
const opts = { skip: URL ? false : 'definí TEST_DATABASE_URL para correr estas pruebas' };

class FakePublisher implements EventPublisher {
  sent: OutgoingMessage[] = [];
  failTimes = 0;
  async publish(m: OutgoingMessage) {
    if (this.failTimes > 0) {
      this.failTimes--;
      throw new Error('kafka caído');
    }
    this.sent.push(m);
  }
}

let pool: Pool;
let pub: FakePublisher;
const meta: MessageMeta = { topic: 'activation.requested', partition: 0, offset: '7', key: 'C-1' };
const newEvent = (customerId = 'C-1') =>
  createEvent('ActivationRequested', {
    correlationId: 'act-0001',
    customerId,
    source: 'test',
    payload: { planId: 'P', channel: 'web', simulateFailure: 'none' },
  });
const processor = (delays = [1, 1, 1]) => new EventProcessor(pool, pub, { consumer: 'test-svc', retryDelaysMs: delays });

before(async () => {
  if (!URL) return;
  await ensureDatabase(URL);
  pool = createPool(URL);
  await initSchema(pool, { outbox: true, extra: ['CREATE TABLE IF NOT EXISTS effects (id serial PRIMARY KEY, event_id text)'] });
});
beforeEach(async () => {
  if (!URL) return;
  pub = new FakePublisher();
  await pool.query('TRUNCATE processed_events, outbox, effects');
});
after(async () => {
  await pool?.end();
});

test('idempotencia: un evento repetido no produce efectos duplicados', opts, async () => {
  const ev = newEvent();
  let calls = 0;
  const handler = async (e: DomainEvent, ctx: any) => {
    calls++;
    await ctx.client.query('INSERT INTO effects (event_id) VALUES ($1)', [e.eventId]);
  };
  const p = processor();
  let commits = 0;
  assert.equal(await p.processMessage(meta, ev, handler, async () => void commits++), 'processed');
  assert.equal(await p.processMessage(meta, ev, handler, async () => void commits++), 'duplicate');
  assert.equal(calls, 1);
  assert.equal((await pool.query('SELECT count(*)::int AS n FROM effects')).rows[0].n, 1);
  assert.equal(commits, 2, 'el duplicado también confirma el offset');
});

test('dos copias simultáneas del mismo evento: una sola se procesa', opts, async () => {
  const ev = newEvent();
  let calls = 0;
  const handler = async () => {
    calls++;
    await new Promise((r) => setTimeout(r, 100));
  };
  const p = processor();
  const [a, b] = await Promise.all([
    p.processMessage(meta, ev, handler, async () => {}),
    p.processMessage(meta, ev, handler, async () => {}),
  ]);
  assert.deepEqual([a, b].sort(), ['duplicate', 'processed']);
  assert.equal(calls, 1);
});

test('si el handler falla se hace ROLLBACK del efecto y del eventId, y se reintenta', opts, async () => {
  const ev = newEvent();
  let calls = 0;
  const handler = async (e: DomainEvent, ctx: any) => {
    calls++;
    await ctx.client.query('INSERT INTO effects (event_id) VALUES ($1)', [e.eventId]);
    await ctx.enqueue(e);
    if (calls < 3) throw new Error('error técnico');
  };
  assert.equal(await processor().processMessage(meta, ev, handler, async () => {}), 'processed');
  assert.equal(calls, 3);
  assert.equal((await pool.query('SELECT count(*)::int AS n FROM effects')).rows[0].n, 1, 'un solo efecto');
  assert.equal((await pool.query('SELECT count(*)::int AS n FROM outbox')).rows[0].n, 1, 'un solo evento en el outbox');
  assert.equal((await pool.query('SELECT count(*)::int AS n FROM processed_events')).rows[0].n, 1);
});

test('tras agotar los reintentos va a la DLQ con el motivo en headers y el offset se confirma', opts, async () => {
  const ev = newEvent();
  let calls = 0;
  const log: string[] = [];
  const handler = async () => {
    calls++;
    log.push('handler');
    throw new Error('boom');
  };
  const out = await processor().processMessage(meta, ev, handler, async () => void log.push('commit'));
  assert.equal(out, 'dlq');
  assert.equal(calls, 4, '1 intento + 3 reintentos');
  assert.deepEqual(log, ['handler', 'handler', 'handler', 'handler', 'commit'], 'commit solo al final');
  assert.equal(pub.sent.length, 1);
  const m = pub.sent[0];
  assert.equal(m.topic, 'activation.requested.dlq');
  assert.deepEqual(m.value, ev, 'se publica el mensaje original');
  assert.equal(m.headers!['x-error-reason'], 'boom');
  assert.equal(m.headers!['x-original-topic'], 'activation.requested');
  assert.equal(m.headers!['x-attempts'], '4');
  assert.equal((await pool.query('SELECT count(*)::int AS n FROM processed_events')).rows[0].n, 0, 'no queda marcado como procesado');
});

test('un mensaje inválido va directo a la DLQ y no llama al handler', opts, async () => {
  let calls = 0;
  let commits = 0;
  const out = await processor().processMessage(meta, 'esto no es JSON', async () => void calls++, async () => void commits++);
  assert.equal(out, 'dlq');
  assert.equal(calls, 0);
  assert.equal(commits, 1);
  assert.equal(pub.sent[0].topic, 'activation.requested.dlq');
  assert.equal(pub.sent[0].value, 'esto no es JSON');
  assert.match(pub.sent[0].headers!['x-error-reason'], /faltan campos/);
});

test('afterCommit solo corre si la transacción hizo COMMIT', opts, async () => {
  const fired: string[] = [];
  let n = 0;
  const handler = async (_e: DomainEvent, ctx: any) => {
    n++;
    ctx.afterCommit(() => void fired.push(`intento-${n}`));
    if (n === 1) throw new Error('falla');
  };
  await processor().processMessage(meta, newEvent(), handler, async () => {});
  assert.deepEqual(fired, ['intento-2']);
});

test('outbox: el relay publica en orden y marca como enviados', opts, async () => {
  const [a, b, c] = [newEvent('C-1'), newEvent('C-2'), newEvent('C-3')];
  for (const e of [a, b, c]) await enqueueEvent(pool, e);
  const relay = new OutboxRelay(pool, pub);
  assert.equal(await relay.tick(), 3);
  assert.deepEqual(pub.sent.map((m) => m.key), ['C-1', 'C-2', 'C-3']);
  assert.equal(pub.sent[0].topic, 'activation.requested');
  assert.equal((await pool.query(`SELECT count(*)::int AS n FROM outbox WHERE status='SENT'`)).rows[0].n, 3);
  assert.equal(await relay.tick(), 0, 'no republica lo ya enviado');
});

test('outbox: si Kafka está caído no se pierde nada y se conserva el orden', opts, async () => {
  for (const id of ['C-1', 'C-2', 'C-3']) await enqueueEvent(pool, newEvent(id));
  const relay = new OutboxRelay(pool, pub);
  pub.failTimes = 1; // falla justo el primero
  await relay.tick();
  assert.equal(pub.sent.length, 0);
  assert.equal((await pool.query(`SELECT count(*)::int AS n FROM outbox WHERE status='PENDING'`)).rows[0].n, 3);
  await relay.tick();
  assert.deepEqual(pub.sent.map((m) => m.key), ['C-1', 'C-2', 'C-3']);
});
