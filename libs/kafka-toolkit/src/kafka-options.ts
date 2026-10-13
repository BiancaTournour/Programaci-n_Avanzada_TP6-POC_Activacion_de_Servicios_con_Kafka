import { Transport, type KafkaOptions } from '@nestjs/microservices';
import { envInt } from './env';
import { kafkaBrokers } from './publisher';

export interface KafkaServerConfig {
  clientId: string;
  groupId: string;
  /** Leer desde el inicio del log si el group no tiene offset. Por defecto true (ver README). */
  fromBeginning?: boolean;
}

/**
 * Opciones del consumidor (transporte Kafka de Nest).
 *  - postfixId '' : Nest agrega "-server" al groupId; se evita para que el group se vea como
 *    "billing-svc" en Kafka UI, igual que en el documento.
 *  - autoCommit false: el offset se confirma a mano, después de procesar (EventProcessor).
 */
export function kafkaServerOptions(cfg: KafkaServerConfig): KafkaOptions {
  const fromBeginning =
    cfg.fromBeginning ?? (process.env.KAFKA_AUTO_OFFSET_RESET ?? 'earliest').toLowerCase() === 'earliest';
  return {
    transport: Transport.KAFKA,
    options: {
      postfixId: '',
      client: {
        clientId: cfg.clientId,
        brokers: kafkaBrokers(),
        retry: { initialRetryTime: 300, retries: 10 },
      },
      consumer: {
        groupId: cfg.groupId,
        allowAutoTopicCreation: false,
        // Riesgo "rebalanceo lento" (sección 11): timeouts cortos para que el escenario 4 sea ágil.
        sessionTimeout: envInt('KAFKA_SESSION_TIMEOUT_MS', 10000),
        heartbeatInterval: envInt('KAFKA_HEARTBEAT_INTERVAL_MS', 3000),
        rebalanceTimeout: 30000,
      },
      subscribe: { fromBeginning },
      run: { autoCommit: false },
    },
  };
}
