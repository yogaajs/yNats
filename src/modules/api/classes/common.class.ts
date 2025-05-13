import type { JetStreamApiError, StreamInfo, ConsumerInfo } from '@nats-io/jetstream';
import type { Client } from "src/core/client.class";
import type { streamConfig, consumerConfig } from '../config';

import Mutex from 'src/classes/mutex.class';
import Manager from 'src/classes/manager.class';
import Logger from 'src/classes/logger.class';

// Types
// ===========================================================

export namespace API {
    export type Options = {
        classType: 'requester' | 'responder',
        streamName: string,
        maxConcurrent?: number,
        debug?: boolean;
    };
    export type StreamConfig = ReturnType<typeof streamConfig>;
    export type ConsumerConfig = ReturnType<typeof consumerConfig>;
};

// Class
// ===========================================================

export default class {
    protected readonly client: Client;
    protected readonly logger: Logger;
    protected readonly manager: Manager;
    protected readonly mutex: Mutex;

    protected readonly classType: API.Options['classType'];
    protected readonly streamName: API.Options['streamName'];

    // Constructor

    constructor(
        client: Client, 
        options: API.Options,
    ) {
        if (!client) throw new Error('Client is required!');
        if (!options.classType) throw new Error('Class type is required!');
        if (!options.streamName) throw new Error('Stream name is required!');

        // Setup class type
        this.classType = options.classType;

        // Setup stream name
        this.streamName = options.streamName;

        // Setup client
        this.client = client;

        // Setup manager
        this.manager = new Manager();

        // Setup mutex
        this.mutex = new Mutex({
            maxConcurrent: options.maxConcurrent ?? 10,
        });

        // Setup logger
        this.logger = new Logger({
            prefix: `[nats][${options.classType}][${options.streamName}]`,
            debug: options.debug ?? false,
        });
    }

    // Private

    protected async isReady(): Promise<void> {
        await this.client.isReady();
        await this.manager.isReady();
    }

    protected async setupStream(config: API.StreamConfig): Promise<StreamInfo> {
        // Ensure client is ready
        await this.client.isReady();

        try {
            // Verify if the stream exists
            return await this.client.stream.info(this.streamName);
        } catch (error) {
            // Create the stream if it doesn't exist
            if ((error as JetStreamApiError).name === "StreamNotFoundError") {
                return await this.client.stream.create(config);
            } else {
                throw error;
            }
        }
    }

    protected async setupConsumer(config: API.ConsumerConfig): Promise<ConsumerInfo> {
        // Ensure client is ready
        await this.client.isReady();

        try {
            // Verify if the consumer exists
            return await this.client.consumer.info(this.streamName, config.durable_name);
        } catch (error) {
            // Create the consumer if it doesn't exist
            if ((error as JetStreamApiError).name === "ConsumerNotFoundError") {
                return await this.client.consumer.create(this.streamName, config);
            } else {
                throw error;
            }
        }
    }
}