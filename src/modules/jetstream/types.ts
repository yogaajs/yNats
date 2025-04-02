import type { StreamConfig, ConsumerConfig } from "@nats-io/jetstream";
import type { WithRequired } from "@nats-io/nats-core/internal";

// Types
// ===========================================================

export interface ProducerConstructor {
    streamConfig: WithRequired<Partial<StreamConfig>, "name">;
};

export interface ConsumerConstructor {
    streamName: string;
    consumerName: string;
    consumerConfig: Partial<ConsumerConfig>;
};

export interface MessagePayload {
    timestamp: number;
    data: Record<string, any>;
}