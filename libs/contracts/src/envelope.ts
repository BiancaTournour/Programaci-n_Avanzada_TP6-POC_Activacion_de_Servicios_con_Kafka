/** Sobre común a todos los eventos. El payload cambia según eventType. */
export interface EventEnvelope<TType extends string = string, TPayload = unknown> {
  /** UUID único; base de la idempotencia. */
  eventId: string;
  eventType: TType;
  /** Versión del contrato de este eventType. */
  version: number;
  /** ISO-8601 UTC. */
  occurredAt: string;
  /** El activationId; agrupa todos los eventos de una activación. */
  correlationId: string;
  /** También es la clave de partición en Kafka (orden por cliente). */
  customerId: string;
  /** Servicio que publicó el evento. */
  source: string;
  payload: TPayload;
}
