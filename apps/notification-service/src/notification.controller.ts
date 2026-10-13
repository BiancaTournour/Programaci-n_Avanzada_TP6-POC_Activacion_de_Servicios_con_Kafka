import { Controller } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';
import { TOPICS } from '@poc/contracts';
import { EventProcessor } from '@poc/kafka-toolkit';
import { NotificationService } from './notification.service';

@Controller()
export class NotificationController {
  constructor(
    private readonly processor: EventProcessor,
    private readonly notifications: NotificationService,
  ) {}

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  onActivationEvent(@Payload() data: unknown, @Ctx() ctx: KafkaContext) {
    return this.processor.handle(ctx, data, (e, c) => this.notifications.onActivationEvent(e, c));
  }
}
