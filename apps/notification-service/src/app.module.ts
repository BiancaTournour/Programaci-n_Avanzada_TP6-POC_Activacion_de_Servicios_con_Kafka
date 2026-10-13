import { Module } from '@nestjs/common';
import { CONSUMER_GROUPS } from '@poc/contracts';
import { ToolkitModule, requireEnv } from '@poc/kafka-toolkit';
import { Mailer, SmtpMailer } from './mailer';
import { NotificationController } from './notification.controller';
import { NotificationService } from './notification.service';
import { NOTIFICATION_SCHEMA } from './schema';

@Module({
  imports: [
    ToolkitModule.forRoot({
      serviceName: 'notification-service',
      groupId: CONSUMER_GROUPS.NOTIFICATION,
      databaseUrl: requireEnv('DATABASE_URL'),
      schema: NOTIFICATION_SCHEMA,
      outbox: false, // no publica nada
    }),
  ],
  controllers: [NotificationController],
  providers: [{ provide: Mailer, useClass: SmtpMailer }, NotificationService],
})
export class AppModule {}
