export const ACTIVATION_SCHEMA = [
  `CREATE SEQUENCE IF NOT EXISTS activation_seq`,
  `CREATE TABLE IF NOT EXISTS activations (
    id               text        PRIMARY KEY,
    customer_id      text        NOT NULL,
    plan_id          text        NOT NULL,
    channel          text        NOT NULL DEFAULT 'web',
    status           text        NOT NULL,
    simulate_failure text        NOT NULL DEFAULT 'none',
    steps            jsonb       NOT NULL DEFAULT '{}'::jsonb,
    history          jsonb       NOT NULL DEFAULT '[]'::jsonb,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now()
  )`,
  `CREATE INDEX IF NOT EXISTS activations_customer_idx ON activations (customer_id)`,
];
