import { Module } from '@nestjs/common';
import { CONSUMER_GROUPS } from '@poc/contracts';
import { ToolkitModule, requireEnv } from '@poc/kafka-toolkit';
import { ProvisioningController } from './provisioning.controller';
import { ProvisioningService } from './provisioning.service';
import { PROVISIONING_SCHEMA } from './schema';

@Module({
  imports: [
    ToolkitModule.forRoot({
      serviceName: 'provisioning-service',
      groupId: CONSUMER_GROUPS.PROVISIONING,
      databaseUrl: requireEnv('DATABASE_URL'),
      schema: PROVISIONING_SCHEMA,
      outbox: true,
    }),
  ],
  controllers: [ProvisioningController],
  providers: [ProvisioningService],
})
export class AppModule {}
