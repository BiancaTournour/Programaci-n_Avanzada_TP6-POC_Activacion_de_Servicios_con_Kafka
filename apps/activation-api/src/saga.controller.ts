import { Controller } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';
import { TOPICS } from '@poc/contracts';
import { EventProcessor } from '@poc/kafka-toolkit';
import { ActivationsService } from './activations.service';

/** Consume los resultados de billing y provisioning (consumer group activation-api). */
@Controller()
export class SagaController {
  constructor(
    private readonly processor: EventProcessor,
    private readonly activations: ActivationsService,
  ) {}

  @EventPattern(TOPICS.BILLING_EVENTS)
  onBilling(@Payload() data: unknown, @Ctx() ctx: KafkaContext) {
    return this.processor.handle(ctx, data, (e, c) => this.activations.applyResult(e, c));
  }

  @EventPattern(TOPICS.PROVISIONING_EVENTS)
  onProvisioning(@Payload() data: unknown, @Ctx() ctx: KafkaContext) {
    return this.processor.handle(ctx, data, (e, c) => this.activations.applyResult(e, c));
  }
}
