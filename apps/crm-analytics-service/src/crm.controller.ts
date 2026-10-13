import { Controller } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';
import { TOPICS } from '@poc/contracts';
import { EventProcessor, type EventHandler } from '@poc/kafka-toolkit';
import { CrmService } from './crm.service';

/** Escucha los 4 topics de negocio sin tocar a ningún otro servicio. */
@Controller()
export class CrmController {
  private readonly handler: EventHandler = (e, c) => this.crm.record(e, c);

  constructor(
    private readonly processor: EventProcessor,
    private readonly crm: CrmService,
  ) {}

  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  onRequested(@Payload() data: unknown, @Ctx() ctx: KafkaContext) {
    return this.processor.handle(ctx, data, this.handler);
  }

  @EventPattern(TOPICS.BILLING_EVENTS)
  onBilling(@Payload() data: unknown, @Ctx() ctx: KafkaContext) {
    return this.processor.handle(ctx, data, this.handler);
  }

  @EventPattern(TOPICS.PROVISIONING_EVENTS)
  onProvisioning(@Payload() data: unknown, @Ctx() ctx: KafkaContext) {
    return this.processor.handle(ctx, data, this.handler);
  }

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  onActivation(@Payload() data: unknown, @Ctx() ctx: KafkaContext) {
    return this.processor.handle(ctx, data, this.handler);
  }
}
