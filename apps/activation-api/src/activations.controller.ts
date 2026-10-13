import { Body, Controller, Get, HttpCode, HttpStatus, Param, Post, Query } from '@nestjs/common';
import { ActivationsService, type CreateActivationInput } from './activations.service';

@Controller()
export class ActivationsController {
  constructor(private readonly activations: ActivationsService) {}

  /** RF-02: 202 Accepted + activationId + PENDING, sin esperar a los demás servicios. */
  @Post('activations')
  @HttpCode(HttpStatus.ACCEPTED)
  create(@Body() body: CreateActivationInput) {
    return this.activations.create(body ?? {});
  }

  /** RF-09: estado actual + historial de eventos. */
  @Get('activations/:id')
  get(@Param('id') id: string) {
    return this.activations.get(id);
  }

  @Get('activations')
  list(@Query('limit') limit?: string) {
    return this.activations.list(limit ? Number(limit) : 20);
  }

  @Get('health')
  health() {
    return { status: 'ok' };
  }
}
