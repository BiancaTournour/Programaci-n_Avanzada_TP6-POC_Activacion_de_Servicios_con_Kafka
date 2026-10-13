import { Module } from '@nestjs/common';
import { CONSUMER_GROUPS } from '@poc/contracts';
import { ToolkitModule, requireEnv } from '@poc/kafka-toolkit';
import { BillingController } from './billing.controller';
import { BillingService } from './billing.service';
import { BILLING_SCHEMA } from './schema';

@Module({
  imports: [
    ToolkitModule.forRoot({
      serviceName: 'billing-service',
      groupId: CONSUMER_GROUPS.BILLING,
      databaseUrl: requireEnv('DATABASE_URL'),
      schema: BILLING_SCHEMA,
      outbox: true,
    }),
  ],
  controllers: [BillingController],
  providers: [BillingService],
})
export class AppModule {}
