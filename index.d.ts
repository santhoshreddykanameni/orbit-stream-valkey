import { BaseAdapter, Message } from "@orbit-stream/core";

export interface ValkeyConfig {
  host?: string;

  port?: number;

  username?: string;

  password?: string;

  batchSize?: number;

  flushInterval?: number;

  maxLen?: number;

  block?: number;

  count?: number;
}

export class ValkeyAdapter extends BaseAdapter {
  constructor(config?: ValkeyConfig);

  connect(): Promise<void>;

  disconnect(): Promise<void>;

  publish(stream: string, message: Message): Promise<void>;

  publishBatch(stream: string, messages: Message[]): Promise<void>;

  subscribe(
    stream: string,
    handler: (messages: any[]) => Promise<void> | void,
    options?: {
      groupId: string;
      consumerId: string;
    },
  ): Promise<void>;
}
