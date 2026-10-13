export const BILLING_SCHEMA = [
  `CREATE TABLE IF NOT EXISTS billing_accounts (
    activation_id text        PRIMARY KEY,           -- índice único por activación
    customer_id   text        NOT NULL,
    account_id    text,
    status        text        NOT NULL,              -- CREATED | CANCELLED | FAILED
    reason        text,
    created_at    timestamptz NOT NULL DEFAULT now(),
    cancelled_at  timestamptz
  )`,
  // Caso borde (sección 06): ActivationFailed llegó antes de que se creara la cuenta.
  `CREATE TABLE IF NOT EXISTS activation_failures (
    activation_id text        PRIMARY KEY,
    seen_at       timestamptz NOT NULL DEFAULT now()
  )`,
];
