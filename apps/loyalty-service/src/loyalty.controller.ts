import { Controller } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';
import { TOPICS } from '@poc/contracts';
import { EventProcessor } from '@poc/kafka-toolkit';
import { LoyaltyService } from './loyalty.service';

@Controller()
export class LoyaltyController {
  constructor(
    private readonly processor: EventProcessor,
    private readonly loyalty: LoyaltyService,
  ) {}

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  onActivationEvent(@Payload() data: unknown, @Ctx() ctx: KafkaContext) {
    return this.processor.handle(ctx, data, (e, c) => this.loyalty.onActivationEvent(e, c));
  }
}
