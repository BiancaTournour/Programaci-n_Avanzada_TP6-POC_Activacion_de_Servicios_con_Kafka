// Solo tipos: el contrato vive en @poc/contracts (única fuente de verdad, RNF-07).
import type { ActivationStatus, EventType, FailureMode, StepStatus } from '@poc/contracts';

export type { ActivationStatus, EventType, FailureMode, StepStatus };

export interface StepResult {
  status: StepStatus;
  at: string;
  reason?: string;
  compensated?: boolean;
}

export interface HistoryEntry {
  eventType: EventType;
  at: string;
  eventId: string;
  source: string;
  detail?: string;
}

export interface Activation {
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
