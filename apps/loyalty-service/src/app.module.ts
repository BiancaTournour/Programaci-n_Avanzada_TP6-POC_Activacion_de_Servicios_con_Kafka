import { Module } from '@nestjs/common';
import { CONSUMER_GROUPS } from '@poc/contracts';
import { ToolkitModule, requireEnv } from '@poc/kafka-toolkit';
import { LoyaltyController } from './loyalty.controller';
import { LoyaltyService } from './loyalty.service';
import { LOYALTY_SCHEMA } from './schema';

@Module({
  imports: [
    ToolkitModule.forRoot({
      serviceName: 'loyalty-service',
      groupId: CONSUMER_GROUPS.LOYALTY,
      databaseUrl: requireEnv('DATABASE_URL'),
      schema: LOYALTY_SCHEMA,
      outbox: false,
    }),
  ],
  controllers: [LoyaltyController],
  providers: [LoyaltyService],
})
export class AppModule {}
