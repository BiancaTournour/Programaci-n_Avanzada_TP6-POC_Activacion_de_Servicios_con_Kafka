import { Module } from '@nestjs/common';
import { CONSUMER_GROUPS } from '@poc/contracts';
import { ToolkitModule, requireEnv } from '@poc/kafka-toolkit';
import { ACTIVATION_NOTIFIER } from './activation.types';
import { ActivationsController } from './activations.controller';
import { ActivationsGateway } from './activations.gateway';
import { ActivationsService } from './activations.service';
import { SagaController } from './saga.controller';
import { ACTIVATION_SCHEMA } from './schema';

@Module({
  imports: [
    ToolkitModule.forRoot({
      serviceName: 'activation-api',
      groupId: CONSUMER_GROUPS.ACTIVATION_API,
      databaseUrl: requireEnv('DATABASE_URL'),
      schema: ACTIVATION_SCHEMA,
      outbox: true,
    }),
  ],
  controllers: [ActivationsController, SagaController],
  providers: [ActivationsGateway, { provide: ACTIVATION_NOTIFIER, useExisting: ActivationsGateway }, ActivationsService],
})
export class AppModule {}
