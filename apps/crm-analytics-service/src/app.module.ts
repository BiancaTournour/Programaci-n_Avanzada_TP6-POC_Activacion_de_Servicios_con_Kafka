import { Module } from '@nestjs/common';
import { CONSUMER_GROUPS } from '@poc/contracts';
import { ToolkitModule, requireEnv } from '@poc/kafka-toolkit';
import { CrmController } from './crm.controller';
import { CrmService } from './crm.service';
import { CRM_SCHEMA } from './schema';

@Module({
  imports: [
    ToolkitModule.forRoot({
      serviceName: 'crm-analytics-service',
      groupId: CONSUMER_GROUPS.CRM_ANALYTICS,
      databaseUrl: requireEnv('DATABASE_URL'),
      schema: CRM_SCHEMA,
      outbox: false,
    }),
  ],
  controllers: [CrmController],
  providers: [CrmService],
})
export class AppModule {}
