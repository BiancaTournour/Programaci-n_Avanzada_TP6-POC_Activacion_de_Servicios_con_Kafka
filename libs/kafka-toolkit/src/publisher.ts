import { Logger, type OnModuleDestroy } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import { lastValueFrom } from 'rxjs';

export interface OutgoingMessage {
  topic: string;
  key: string;
  /** Objeto (se serializa a JSON) o string/Buffer crudo (para reenviar mensajes inválidos a la DLQ). */
  value: unknown;
  headers?: Record<string, string>;
}

/**
 * Abstracción del productor. El resto de la librería (outbox, DLQ) solo conoce esta interfaz,
 * así que cambiar kafkajs por el cliente de Confluent toca únicamente este archivo.
 */
export interface EventPublisher {
  publish(message: OutgoingMessage): Promise<void>;
}

export function kafkaBrokers(): string[] {
  return (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',').map((s) => s.trim());
}

/** Productor sobre ClientKafka de @nestjs/microservices (decisión técnica 4 del documento). */
export class KafkaPublisher implements EventPublisher, OnModuleDestroy {
  private readonly log = new Logger(KafkaPublisher.name);
  private readonly client: ClientKafka;

  constructor(clientId: string) {
    this.client = new ClientKafka({
      client: { clientId, brokers: kafkaBrokers(), retry: { initialRetryTime: 300, retries: 10 } },
      producer: { allowAutoTopicCreation: false },
      producerOnlyMode: true,
    });
  }

  async connect(): Promise<void> {
    await this.client.connect();
    this.log.log(`Productor conectado a ${kafkaBrokers().join(',')}`);
  }

  async publish(message: OutgoingMessage): Promise<void> {
    // emit() devuelve un observable "caliente": defaultValue evita EmptyError si ya completó.
    await lastValueFrom(
      this.client.emit(message.topic, { key: message.key, value: message.value, headers: message.headers }),
      { defaultValue: undefined },
    );
  }

  async onModuleDestroy(): Promise<void> {
    await this.client.close();
  }
}
