import type { StreamConfig, ConsumerConfig, RetentionPolicy, StorageType } from 'nats';

// Types
// ===========================================================

export interface ProducerConstructor {
    streamConfig: Partial<StreamConfig>;
};

export interface ConsumerConstructor {
    streamName: string;
    consumerName: string;
    consumerConfig?: Partial<ConsumerConfig>;
};

export interface MessagePayload {
    timestamp: number;
    data: Record<string, any>;
}