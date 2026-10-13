import type { ActivationStatus, EventType, FailureMode, StepStatus } from '@poc/contracts';

export interface StepResult {
  status: StepStatus;
  at: string;
  reason?: string;
  /** billing: la cuenta fue anulada por compensación. */
  compensated?: boolean;
}

export interface HistoryEntry {
  eventType: EventType;
  at: string;
  eventId: string;
  source: string;
  detail?: string;
}

/** Forma del documento de activación (sección 07 del documento) que ven la API y la UI. */
export interface ActivationView {
  id: string;
  customerId: string;
  planId: string;
  channel: string;
  status: ActivationStatus;
  simulateFailure: FailureMode;
  steps: { billing?: StepResult; provisioning?: StepResult };
  history: HistoryEntry[];
  createdAt: string;
  updatedAt: string;
}

/** Quien recibe cada cambio de estado (el gateway de WebSocket). */
export interface ActivationNotifier {
  publish(activation: ActivationView): void;
}
export const ACTIVATION_NOTIFIER = 'ACTIVATION_NOTIFIER';
