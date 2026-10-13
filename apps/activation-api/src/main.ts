import 'reflect-metadata';
import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { CONSUMER_GROUPS } from '@poc/contracts';
import { kafkaServerOptions } from '@poc/kafka-toolkit';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  app.enableCors({ origin: true });
  app.enableShutdownHooks();
  app.connectMicroservice(
    kafkaServerOptions({
      clientId: process.env.KAFKA_CLIENT_ID ?? 'activation-api',
      groupId: process.env.KAFKA_GROUP_ID ?? CONSUMER_GROUPS.ACTIVATION_API,
    }),
  );
  await app.startAllMicroservices();
  const port = Number(process.env.PORT ?? 3000);
  await app.listen(port, '0.0.0.0');
  new Logger('activation-api').log(`REST + WebSocket en :${port}`);
}
bootstrap();
