import { Controller } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';
import { TOPICS } from '@poc/contracts';
import { EventProcessor } from '@poc/kafka-toolkit';
import { ProvisioningService } from './provisioning.service';

@Controller()
export class ProvisioningController {
  constructor(
    private readonly processor: EventProcessor,
    private readonly provisioning: ProvisioningService,
  ) {}

  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  onRequested(@Payload() data: unknown, @Ctx() ctx: KafkaContext) {
    return this.processor.handle(ctx, data, (e, c) => this.provisioning.onActivationRequested(e, c));
  }
}
