/** Topics de negocio. Convención: dominio.tipo */
export const TOPICS = {
  ACTIVATION_REQUESTED: 'activation.requested',
  BILLING_EVENTS: 'billing.events',
  PROVISIONING_EVENTS: 'provisioning.events',
  ACTIVATION_EVENTS: 'activation.events',
} as const;

export type Topic = (typeof TOPICS)[keyof typeof TOPICS];

/** Nombre de la DLQ de un topic: <topic>.dlq */
export const dlqOf = (topic: string): string => `${topic}.dlq`;

export const BUSINESS_TOPICS: Topic[] = Object.values(TOPICS);
export const DLQ_TOPICS: string[] = BUSINESS_TOPICS.map(dlqOf);
export const ALL_TOPICS: string[] = [...BUSINESS_TOPICS, ...DLQ_TOPICS];

/** Configuración común de todos los topics (usada por docker/create-topics.sh). */
export const TOPIC_CONFIG = {
  partitions: 3,
  retentionMs: 7 * 24 * 60 * 60 * 1000, // 7 días
} as const;

/** Consumer groups (un group por servicio). */
export const CONSUMER_GROUPS = {
  ACTIVATION_API: 'activation-api',
  BILLING: 'billing-svc',
  PROVISIONING: 'provisioning-svc',
  NOTIFICATION: 'notification-svc',
  CRM_ANALYTICS: 'crm-analytics',
  LOYALTY: 'loyalty-svc', // escenario 5 de la demo
} as const;

/** Nombres de servicio usados en el campo `source` del sobre. */
export const SERVICES = {
  ACTIVATION_API: 'activation-api',
  BILLING: 'billing',
  PROVISIONING: 'provisioning',
} as const;
