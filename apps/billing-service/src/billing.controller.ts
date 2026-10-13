import { Controller } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';
import { TOPICS } from '@poc/contracts';
import { EventProcessor } from '@poc/kafka-toolkit';
import { BillingService } from './billing.service';

@Controller()
export class BillingController {
  constructor(
    private readonly processor: EventProcessor,
    private readonly billing: BillingService,
  ) {}

  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  onRequested(@Payload() data: unknown, @Ctx() ctx: KafkaContext) {
    return this.processor.handle(ctx, data, (e, c) => this.billing.onActivationRequested(e, c));
  }

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  onActivationEvent(@Payload() data: unknown, @Ctx() ctx: KafkaContext) {
    return this.processor.handle(ctx, data, (e, c) => this.billing.onActivationEvent(e, c));
  }
}
