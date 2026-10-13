export const CRM_SCHEMA = [
  `CREATE TABLE IF NOT EXISTS event_log (
    event_id        text        PRIMARY KEY,         -- eventId único
    correlation_id  text        NOT NULL,
    customer_id     text        NOT NULL,
    event_type      text        NOT NULL,
    version         int         NOT NULL,
    source          text        NOT NULL,
    topic           text        NOT NULL,
    kafka_partition int         NOT NULL,
    kafka_offset    bigint      NOT NULL,
    occurred_at     timestamptz NOT NULL,
    received_at     timestamptz NOT NULL DEFAULT now(),
    payload         jsonb       NOT NULL
  )`,
  `CREATE INDEX IF NOT EXISTS event_log_correlation_idx ON event_log (correlation_id)`,
];
