export const LOYALTY_SCHEMA = [
  `CREATE TABLE IF NOT EXISTS loyalty_points (
    customer_id text        PRIMARY KEY,
    points      int         NOT NULL DEFAULT 0,
    activations int         NOT NULL DEFAULT 0,
    updated_at  timestamptz NOT NULL DEFAULT now()
  )`,
];
