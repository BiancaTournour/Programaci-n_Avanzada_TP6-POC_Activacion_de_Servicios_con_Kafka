import 'reflect-metadata';
import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { CONSUMER_GROUPS } from '@poc/contracts';
import { kafkaServerOptions } from '@poc/kafka-toolkit';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.createMicroservice(
    AppModule,
    kafkaServerOptions({
      clientId: process.env.KAFKA_CLIENT_ID ?? 'billing-service',
      groupId: process.env.KAFKA_GROUP_ID ?? CONSUMER_GROUPS.BILLING,
    }),
  );
  app.enableShutdownHooks(); // sale del consumer group limpiamente (rebalanceo rápido)
  await app.listen();
  new Logger('billing-service').log('consumidor Kafka iniciado');
}
bootstrap();
