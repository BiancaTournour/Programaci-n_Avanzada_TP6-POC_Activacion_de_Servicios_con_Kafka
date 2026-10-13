export const NOTIFICATION_SCHEMA = [
  `CREATE TABLE IF NOT EXISTS notifications (
    id            bigserial   PRIMARY KEY,
    activation_id text        NOT NULL,
    event_type    text        NOT NULL,
    to_address    text        NOT NULL,
    subject       text        NOT NULL,
    sent_at       timestamptz NOT NULL DEFAULT now(),
    UNIQUE (activation_id, event_type)     -- un email por evento, aunque el evento se repita
  )`,
];
