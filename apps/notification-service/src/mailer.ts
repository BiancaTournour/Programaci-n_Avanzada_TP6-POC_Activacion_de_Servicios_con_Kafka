import { Injectable } from '@nestjs/common';
import { createTransport, type Transporter } from 'nodemailer';

export interface MailMessage {
  to: string;
  subject: string;
  text: string;
  html: string;
}

/** Abstracción del envío, para poder reemplazar Mailhog o probar sin SMTP. */
export abstract class Mailer {
  abstract send(message: MailMessage): Promise<void>;
}

@Injectable()
export class SmtpMailer extends Mailer {
  private readonly transport: Transporter = createTransport({
    host: process.env.SMTP_HOST ?? 'localhost',
    port: Number(process.env.SMTP_PORT ?? 1025),
    secure: false,
    ignoreTLS: true,
  });

  async send(message: MailMessage): Promise<void> {
    await this.transport.sendMail({ from: process.env.MAIL_FROM ?? 'activaciones@poc.local', ...message });
  }
}
