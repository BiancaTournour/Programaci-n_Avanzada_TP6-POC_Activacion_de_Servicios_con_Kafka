export const PROVISIONING_SCHEMA = [
  `CREATE TABLE IF NOT EXISTS provisioning_orders (
    activation_id text        PRIMARY KEY,           -- índice único por activación
    customer_id   text        NOT NULL,
    order_id      text,
    status        text        NOT NULL,              -- COMPLETED | FAILED
    reason        text,
    created_at    timestamptz NOT NULL DEFAULT now()
  )`,
];
