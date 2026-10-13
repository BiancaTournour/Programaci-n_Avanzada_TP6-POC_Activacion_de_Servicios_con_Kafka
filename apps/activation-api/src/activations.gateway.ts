import { Injectable } from '@nestjs/common';
import { WebSocketGateway, WebSocketServer } from '@nestjs/websockets';
import type { Server } from 'socket.io';
import type { ActivationNotifier, ActivationView } from './activation.types';

/** RF-08: envía cada cambio de una activación a la UI. */
@Injectable()
@WebSocketGateway({ cors: { origin: '*' } })
export class ActivationsGateway implements ActivationNotifier {
  @WebSocketServer() private server!: Server;

  publish(activation: ActivationView): void {
    this.server?.emit('activation.updated', activation);
  }
}
