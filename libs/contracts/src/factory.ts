import { randomUUID } from 'node:crypto';
import { EVENT_TOPIC, EVENT_VERSIONS, isEventType } from './events';
import type { DomainEvent, EventPayloads, EventType } from './events';
import type { Topic } from './topics';

export interface CreateEventInput<T extends EventType> {
  correlationId: string;
  customerId: string;
  source: string;
  payload: EventPayloads[T];
}

/** Crea un evento con el sobre completo (eventId, versión y fecha automáticos). */
export function createEvent<T extends EventType>(
  eventType: T,
  input: CreateEventInput<T>,
): DomainEvent<T> {
  return {
    eventId: randomUUID(),
    eventType,
    version: EVENT_VERSIONS[eventType],
    occurredAt: new Date().toISOString(),
    correlationId: input.correlationId,
    customerId: input.customerId,
    source: input.source,
    payload: input.payload,
  } as DomainEvent<T>;
}

/** Clave de partición del mensaje: siempre customerId (RNF-02). */
export const partitionKey = (event: { customerId: string }): string => event.customerId;

/** Topic donde se publica un evento. */
export const topicOf = (eventType: EventType): Topic => EVENT_TOPIC[eventType];

/** Valida la forma mínima del sobre al consumir. Lanza Error si es inválido. */
export function parseEvent(raw: unknown): DomainEvent {
  const e = raw as Record<string, unknown> | null;
  const missing = ['eventId', 'eventType', 'version', 'occurredAt', 'correlationId', 'customerId', 'source', 'payload']
    .filter((k) => !e || e[k] === undefined || e[k] === null);
  if (missing.length) {
    throw new Error(`Evento inválido, faltan campos: ${missing.join(', ')}`);
  }
  if (!isEventType(e!.eventType)) {
    throw new Error(`eventType desconocido: ${String(e!.eventType)}`);
  }
  return e as unknown as DomainEvent;
}
