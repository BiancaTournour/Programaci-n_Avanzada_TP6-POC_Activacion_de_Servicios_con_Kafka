import { useEffect, useMemo, useState, type FormEvent } from 'react';
import { API_URL, KAFKA_UI_URL, MAILHOG_URL, connectSocket, createActivation, listActivations } from './api';
import type { Activation, EventType, FailureMode, HistoryEntry, StepResult } from './types';

const PLANS = ['FLOW-FULL', 'FLOW-LITE', 'FLOW-MOBILE'];
const FAILURE_OPTIONS: { value: FailureMode; label: string; hint: string }[] = [
  { value: 'none', label: 'Sin fallo', hint: 'Camino feliz' },
  { value: 'billing', label: 'Falla billing', hint: 'BillingFailed' },
  { value: 'provisioning', label: 'Falla provisioning', hint: 'Compensación en billing' },
];

type Tone = 'ok' | 'fail' | 'comp' | 'info';
const EVENT_TONE: Record<EventType, Tone> = {
  ActivationRequested: 'info',
  BillingAccountCreated: 'ok',
  ProvisioningCompleted: 'ok',
  ActivationCompleted: 'ok',
  BillingFailed: 'fail',
  ProvisioningFailed: 'fail',
  ActivationFailed: 'fail',
  BillingAccountCancelled: 'comp',
};

const time = (iso: string) => new Date(iso).toLocaleTimeString('es-AR', { hour12: false });
let counter = 1234;

export function App() {
  const [activations, setActivations] = useState<Record<string, Activation>>({});
  const [connected, setConnected] = useState(false);
  const [customerId, setCustomerId] = useState('C-1234');
  const [planId, setPlanId] = useState(PLANS[0]);
  const [failure, setFailure] = useState<FailureMode>('none');
  const [sending, setSending] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [lastAccepted, setLastAccepted] = useState<string | null>(null);

  const upsert = (a: Activation) =>
    setActivations((prev) => {
      const old = prev[a.id];
      // Las actualizaciones pueden llegar desordenadas: gana la más nueva.
      if (old && old.updatedAt > a.updatedAt) return prev;
      return { ...prev, [a.id]: a };
    });

  useEffect(() => {
    const socket = connectSocket(upsert, setConnected);
    listActivations()
      .then((list) => list.forEach(upsert))
      .catch((e) => setError(`No se pudo conectar con activation-api (${API_URL}): ${e.message}`));
    return () => void socket.close();
  }, []);

  const sorted = useMemo(
    () => Object.values(activations).sort((a, b) => b.createdAt.localeCompare(a.createdAt) || b.id.localeCompare(a.id)),
    [activations],
  );

  async function onSubmit(e: FormEvent) {
    e.preventDefault();
    setSending(true);
    setError(null);
    try {
      const r = await createActivation({ customerId: customerId.trim(), planId, simulateFailure: failure });
      setLastAccepted(`202 Accepted · ${r.activationId} · ${r.status}`);
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setSending(false);
    }
  }

  return (
    <main>
      <header className="top">
        <div>
          <h1>Activación de servicios</h1>
          <p className="muted">Un evento en Kafka dispara todo el flujo</p>
        </div>
        <div className="top-links">
          <span className={`conn ${connected ? 'on' : 'off'}`} title="WebSocket con activation-api">
            {connected ? 'en vivo' : 'sin conexión'}
          </span>
          <a href={KAFKA_UI_URL} target="_blank" rel="noreferrer">Kafka UI</a>
          <a href={MAILHOG_URL} target="_blank" rel="noreferrer">Mailhog</a>
        </div>
      </header>

      <form className="card" onSubmit={onSubmit}>
        <h2>Contratar plan</h2>
        <div className="row">
          <label>
            Cliente (customerId)
            <div className="inline">
              <input value={customerId} onChange={(e) => setCustomerId(e.target.value)} required />
              <button type="button" className="ghost" onClick={() => setCustomerId(`C-${++counter}`)} title="Cliente nuevo">
                nuevo
              </button>
            </div>
          </label>
          <label>
            Plan (planId)
            <select value={planId} onChange={(e) => setPlanId(e.target.value)}>
              {PLANS.map((p) => (
                <option key={p}>{p}</option>
              ))}
            </select>
          </label>
        </div>
        <fieldset>
          <legend>Fallo forzado</legend>
          <div className="radios">
            {FAILURE_OPTIONS.map((o) => (
              <label key={o.value} className={`radio ${failure === o.value ? 'sel' : ''}`}>
                <input type="radio" name="failure" checked={failure === o.value} onChange={() => setFailure(o.value)} />
                <span>{o.label}</span>
                <small>{o.hint}</small>
              </label>
            ))}
          </div>
        </fieldset>
        <div className="actions">
          <button type="submit" disabled={sending || !customerId.trim()}>
            {sending ? 'Enviando…' : 'Contratar plan'}
          </button>
          {lastAccepted && <span className="muted mono">{lastAccepted}</span>}
        </div>
        {error && <p className="error">{error}</p>}
      </form>

      <section>
        <h2>Timeline de eventos</h2>
        {sorted.length === 0 && <p className="muted">Todavía no hay activaciones. Contratá un plan para ver los eventos pasar.</p>}
        {sorted.map((a) => (
          <ActivationCard key={a.id} a={a} />
        ))}
      </section>
    </main>
  );
}

function stepLabel(name: string, s: StepResult | undefined): { text: string; tone: Tone | 'wait' } {
  if (!s) return { text: `${name}: esperando`, tone: 'wait' };
  if (s.compensated) return { text: `${name}: anulada`, tone: 'comp' };
  return s.status === 'OK' ? { text: `${name}: OK`, tone: 'ok' } : { text: `${name}: falló`, tone: 'fail' };
}

function ActivationCard({ a }: { a: Activation }) {
  const billing = stepLabel('Facturación', a.steps.billing);
  const prov = stepLabel('Aprovisionamiento', a.steps.provisioning);
  return (
    <article className={`card act ${a.status.toLowerCase()}`}>
      <div className="act-head">
        <div>
          <strong className="mono">{a.id}</strong> <span className="muted">· {a.customerId} · {a.planId}</span>
        </div>
        <span className={`badge ${a.status.toLowerCase()}`}>{a.status}</span>
      </div>
      <div className="chips">
        <span className={`chip ${billing.tone}`}>{billing.text}</span>
        <span className={`chip ${prov.tone}`}>{prov.text}</span>
        {a.simulateFailure !== 'none' && <span className="chip wait">fallo forzado: {a.simulateFailure}</span>}
      </div>
      <ol className="timeline">
        {a.history.map((h) => (
          <TimelineItem key={h.eventId} h={h} />
        ))}
        {(a.status === 'PENDING' || a.status === 'IN_PROGRESS') && (
          <li className="tl pending">
            <span className="dot" />
            <span className="muted">esperando próximos eventos…</span>
          </li>
        )}
      </ol>
    </article>
  );
}

function TimelineItem({ h }: { h: HistoryEntry }) {
  return (
    <li className={`tl ${EVENT_TONE[h.eventType]}`}>
      <span className="dot" />
      <div>
        <div>
          <span className="ev">{h.eventType}</span> <span className="muted mono">{time(h.at)} · {h.source}</span>
        </div>
        {h.detail && <div className="muted small">{h.detail}</div>}
      </div>
    </li>
  );
}
