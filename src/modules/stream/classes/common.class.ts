import type { JetStreamApiError, StreamInfo, ConsumerInfo } from '@nats-io/jetstream';
import type { Client } from "src/core/client.class";
import type { StreamPublisher } from '../modules/publisher.class';
import type { StreamConsumer } from '../modules/consumer.class';
import { streamConfig, consumerConfig } from '../config';

import Manager from 'src/classes/manager.class';
import Logger from 'src/classes/logger.class';

// Class
// ===========================================================

export class Common<Type extends 'publisher' | 'consumer'> {
    protected readonly client: Client;
    protected readonly logger: Logger;
    protected readonly manager: Manager;

    protected readonly streamConfig: ReturnType<typeof streamConfig>;
    protected readonly consumerConfig: Type extends 'consumer' ? ReturnType<typeof consumerConfig> : undefined;

    // Constructor

    constructor(
        client: Client, 
        config: Type extends 'consumer' ? StreamConsumer.Config : StreamPublisher.Config
    ) {
        if (!client) throw new Error('Client is required!');
        if (!config) throw new Error('Config is required!');

        this.client = client;
        this.manager = new Manager();

        // Always assign streamConfig
        this.streamConfig = streamConfig({
            streamName: config.streamName,
            streamRetention: config.streamRetention,
            streamStorage: config.streamStorage,
            streamMaxConsumers: config.streamMaxConsumers,
            streamMaxAgeSeconds: config.streamMaxAgeSeconds,
            streamMaxMegabytes: config.streamMaxMegabytes,
        });

        // Only assign consumerConfig and logger if Type is 'consumer'
        if ('consumerName' in config) {
            this.consumerConfig = consumerConfig({
                streamName: config.streamName,
                streamSubject: config.streamSubject,
                consumerName: config.consumerName,
            }) as any;
            this.logger = new Logger({
                prefix: `[nats][consumer][${this.consumerConfig!.durable_name}]`,
            });
        } else {
            this.consumerConfig = undefined as any;
            this.logger = new Logger({
                prefix: `[nats][publisher][${this.streamConfig.name}]`,
            });
        }
    }

    // Private

    protected async isReady(): Promise<void> {
        await this.client.isReady();
        await this.manager.isReady();
    }

    protected async setupStream(): Promise<StreamInfo> {
        const { streamConfig } = this;

        // Ensure client is ready
        await this.client.isReady();

        try {
            // Verify if the stream exists
            return await this.client.stream.info(streamConfig.name);
        } catch (error) {
            // Create the stream if it doesn't exist
            if ((error as JetStreamApiError).name === "StreamNotFoundError") {
                return await this.client.stream.create(streamConfig);
            } else {
                throw error;
            }
        }
    }

    protected async setupConsumer(): Promise<ConsumerInfo> {
        const { streamConfig, consumerConfig } = this;

        // Ensure client is ready
        await this.client.isReady();

        try {
            // Verify if the consumer exists
            return await this.client.consumer.info(streamConfig.name, consumerConfig!.durable_name);
        } catch (error) {
            // Create the consumer if it doesn't exist
            if ((error as JetStreamApiError).name === "ConsumerNotFoundError") {
                return await this.client.consumer.create(streamConfig.name, consumerConfig!);
            } else {
                throw error;
            }
        }
    }
}