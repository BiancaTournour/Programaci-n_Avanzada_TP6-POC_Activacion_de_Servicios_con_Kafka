import { io, type Socket } from 'socket.io-client';
import type { Activation, FailureMode } from './types';

export const API_URL: string = import.meta.env.VITE_API_URL ?? 'http://localhost:3000';
export const KAFKA_UI_URL: string = import.meta.env.VITE_KAFKA_UI_URL ?? 'http://localhost:8080';
export const MAILHOG_URL: string = import.meta.env.VITE_MAILHOG_URL ?? 'http://localhost:8025';

export interface CreateActivationBody {
  customerId: string;
  planId: string;
  simulateFailure: FailureMode;
}

/** RF-01 / RF-02: POST /activations responde 202 con el activationId y PENDING. */
export async function createActivation(body: CreateActivationBody): Promise<{ activationId: string; status: string }> {
  const res = await fetch(`${API_URL}/activations`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error(Array.isArray(err.message) ? err.message.join(', ') : err.message ?? `HTTP ${res.status}`);
  }
  return res.json();
}

export async function listActivations(): Promise<Activation[]> {
  const res = await fetch(`${API_URL}/activations?limit=20`);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

/** RF-08: cada cambio de una activación llega por WebSocket. */
export function connectSocket(onUpdate: (a: Activation) => void, onStatus: (connected: boolean) => void): Socket {
  const socket = io(API_URL, { transports: ['websocket', 'polling'] });
  socket.on('connect', () => onStatus(true));
  socket.on('disconnect', () => onStatus(false));
  socket.on('activation.updated', onUpdate);
  return socket;
}
