import type { EventEnvelope } from './envelope';
import { TOPICS, type Topic } from './topics';

// ---------- Tipos de dominio ----------
export type FailureMode = 'none' | 'billing' | 'provisioning';
export type ActivationStatus = 'PENDING' | 'IN_PROGRESS' | 'ACTIVE' | 'FAILED';
export type StepStatus = 'PENDING' | 'OK' | 'FAILED';

// ---------- Payloads ----------
export interface ActivationRequestedPayload {
  planId: string;
  channel: string;
  /** Solo para la demo: indica a billing o provisioning que deben fallar. */
  simulateFailure: FailureMode;
}
export interface BillingAccountCreatedPayload {
  accountId: string;
}
export interface BillingFailedPayload {
  reason: string;
}
export interface BillingAccountCancelledPayload {
  accountId: string;
  reason: string;
}
export interface ProvisioningCompletedPayload {
  orderId: string;
}
export interface ProvisioningFailedPayload {
  reason: string;
}
export interface ActivationCompletedPayload {
  planId: string;
}
export interface ActivationFailedPayload {
  failedStep: 'billing' | 'provisioning';
  reason: string;
}

// ---------- Mapa eventType -> payload ----------
export interface EventPayloads {
  ActivationRequested: ActivationRequestedPayload;
  BillingAccountCreated: BillingAccountCreatedPayload;
  BillingFailed: BillingFailedPayload;
  BillingAccountCancelled: BillingAccountCancelledPayload;
  ProvisioningCompleted: ProvisioningCompletedPayload;
  ProvisioningFailed: ProvisioningFailedPayload;
  ActivationCompleted: ActivationCompletedPayload;
  ActivationFailed: ActivationFailedPayload;
}

export type EventType = keyof EventPayloads;

export type DomainEvent<T extends EventType = EventType> = {
  [K in T]: EventEnvelope<K, EventPayloads[K]>;
}[T];

// ---------- Versión vigente de cada contrato (RNF-07) ----------
export const EVENT_VERSIONS: Record<EventType, number> = {
  ActivationRequested: 1,
  BillingAccountCreated: 1,
  BillingFailed: 1,
  BillingAccountCancelled: 1,
  ProvisioningCompleted: 1,
  ProvisioningFailed: 1,
  ActivationCompleted: 1,
  ActivationFailed: 1,
};

// ---------- Topic al que pertenece cada evento ----------
export const EVENT_TOPIC: Record<EventType, Topic> = {
  ActivationRequested: TOPICS.ACTIVATION_REQUESTED,
  BillingAccountCreated: TOPICS.BILLING_EVENTS,
  BillingFailed: TOPICS.BILLING_EVENTS,
  BillingAccountCancelled: TOPICS.BILLING_EVENTS,
  ProvisioningCompleted: TOPICS.PROVISIONING_EVENTS,
  ProvisioningFailed: TOPICS.PROVISIONING_EVENTS,
  ActivationCompleted: TOPICS.ACTIVATION_EVENTS,
  ActivationFailed: TOPICS.ACTIVATION_EVENTS,
};

export const EVENT_TYPES = Object.keys(EVENT_VERSIONS) as EventType[];

export const isEventType = (value: unknown): value is EventType =>
  typeof value === 'string' && value in EVENT_VERSIONS;
