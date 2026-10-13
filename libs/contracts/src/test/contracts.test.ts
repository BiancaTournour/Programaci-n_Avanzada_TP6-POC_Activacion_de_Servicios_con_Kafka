import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import {
  createEvent, parseEvent, partitionKey, topicOf, dlqOf, ALL_TOPICS, TOPICS, EVENT_TYPES, EVENT_TOPIC, BUSINESS_TOPICS, TOPIC_CONFIG,
} from '../index';

test('createEvent arma el sobre completo', () => {
  const ev = createEvent('ActivationRequested', {
    correlationId: 'act-0001',
    customerId: 'C-1234',
    source: 'activation-api',
    payload: { planId: 'FLOW-FULL', channel: 'web', simulateFailure: 'none' },
  });
  assert.equal(ev.eventType, 'ActivationRequested');
  assert.equal(ev.version, 1);
  assert.match(ev.eventId, /^[0-9a-f-]{36}$/);
  assert.equal(partitionKey(ev), 'C-1234');
  assert.equal(topicOf(ev.eventType), TOPICS.ACTIVATION_REQUESTED);
});

test('parseEvent rechaza eventos inválidos', () => {
  assert.throws(() => parseEvent({}), /faltan campos/);
  assert.throws(
    () => parseEvent({ eventId: 'x', eventType: 'Nope', version: 1, occurredAt: 'x', correlationId: 'x', customerId: 'x', source: 'x', payload: {} }),
    /desconocido/,
  );
});

test('todos los eventos tienen topic y hay 4 topics + 4 DLQ', () => {
  for (const t of EVENT_TYPES) assert.ok(EVENT_TOPIC[t]);
  assert.equal(ALL_TOPICS.length, 8);
  assert.equal(dlqOf('billing.events'), 'billing.events.dlq');
});

test('docker/create-topics.sh coincide con los topics de contracts', () => {
  // dist/test -> libs/contracts -> libs -> raíz
  const script = readFileSync(resolve(__dirname, '../../../../docker/create-topics.sh'), 'utf8');
  const block = script.match(/TOPICS=\(([\s\S]*?)\)/)![1];
  const inScript = block.split('\n').map((l) => l.trim()).filter(Boolean).sort();
  assert.deepEqual(inScript, [...BUSINESS_TOPICS].sort());
  assert.match(script, new RegExp(`PARTITIONS=${TOPIC_CONFIG.partitions}\\b`));
  assert.match(script, new RegExp(`RETENTION_MS=${TOPIC_CONFIG.retentionMs}\\b`));
});
