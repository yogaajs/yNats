import type { IConsumers } from '../types';
import type { Consumer as JetStreamConsumer, ConsumerInfo } from '@nats-io/jetstream';
import { AckPolicy, DeliverPolicy, ConsumerConfig } from '@nats-io/jetstream';
import Logger from 'src/classes/logger.class';
import Manager from 'src/classes/manager.class';
import { withExpiration } from 'src/utils/timeout.utils';

// Constants
// ===========================================================

export const DEFAULT_ACK_WAIT_MS: number = 10 * 1_000; // 10s

// Class
// ===========================================================

export class Consumers {
    protected readonly logger: Logger;

    protected readonly client: IConsumers['client'];
    protected readonly stream: IConsumers['stream'];

    // Constructor

    constructor({ client, stream }: IConsumers) {
        if (!client) {
            throw new Error("Client is required!");
        }
        if (!stream) {
            throw new Error("Stream is required!");
        }

        // Modules
        this.logger = new Logger(`[nats][consumers]`);

        // Private
        this.client = client;
        this.stream = stream;
    }

    // Public

    public async list(): Promise<ConsumerInfo[]> {
        // Constants
        const { stream, client } = this;

        // Wait for resources to be ready
        await client.isReady();
        await stream.isReady();

        // Constants
        const { name: streamName } = stream.config;
        const { jetstreamManager } = client;

        // Get the list of consumers
        return await jetstreamManager.consumers.list(streamName).next();
    }

    public async consumerInfo(consumerName: string): Promise<JetStreamConsumer> {
        // Constants
        const { stream, client } = this;

        // Wait for resources to be ready
        await client.isReady();
        await stream.isReady();

        // Setup the consumer
        const { name: streamName } = stream.config;
        const { jetstreamManager, jetstreamClient } = client;

        // Check if consumer exists.
        const info = await jetstreamManager.consumers.info(streamName, consumerName);

        // Retrieve the consumer.
        const consumer = await jetstreamClient.consumers.get(streamName, consumerName);
        return consumer;
    }

    public async getConsumer(config: Partial<ConsumerConfig>): Promise<JetStreamConsumer> {
        // Constants
        const { stream, client } = this;

        // Wait for resources to be ready
        await client.isReady();
        await stream.isReady();

        // Setup the consumer
        const { name: streamName } = stream.config;
        const { jetstreamManager, jetstreamClient } = client;

        // Setup the consumer
        const defaultConfig = this.getDefaultConfig(config);

        // Create the consumer on the stream.
        const info = await jetstreamManager.consumers.add(streamName, defaultConfig);

        // Log the creation
        this.logger.info(`Consumer (${info.name}) created for stream (${streamName})!`);

        // Retrieve the consumer.
        const consumer = await jetstreamClient.consumers.get(streamName, info.name);
        return consumer;
    }

    public async getEphemeralConsumer(config: Partial<ConsumerConfig>): Promise<JetStreamConsumer> {
        // Constants
        const { stream, client } = this;

        // Wait for resources to be ready
        await client.isReady();
        await stream.isReady();

        // Setup the consumer
        const { name: streamName } = stream.config;
        const { jetstreamManager, jetstreamClient } = client;

        // Setup the consumer
        const defaultConfig = this.getDefaultConfig(config);
        defaultConfig.durable_name = undefined;

        // Create the consumer on the stream.
        const info = await jetstreamManager.consumers.add(streamName, defaultConfig);

        // Log the creation
        this.logger.info(`Ephemeral consumer (${info.name}) created for stream (${streamName})!`);

        // Retrieve the consumer.
        const consumer = await jetstreamClient.consumers.get(streamName, info.name);
        return consumer;
    }

    public async consumerSetup(config: Partial<ConsumerConfig>): Promise<JetStreamConsumer> {
        // Constants
        const { durable_name: consumerName } = config;
        const { name: streamName } = this.stream.config;
        const { jetstreamManager, jetstreamClient } = this.client;

        try {
            // Check if consumer exists.
            const info = await jetstreamManager.consumers.info(streamName, consumerName!);

            // Merge the current config with the new configuration.
            // New config values in this.consumerConfig will overwrite the current config values.
            const updatedConfig = { ...info.config, ...config };
            updatedConfig.durable_name = consumerName;

            // Update the consumer with the merged configuration.
            await jetstreamManager.consumers.update(streamName, consumerName!, updatedConfig);

            // Log the update
            this.logger.info(`Consumer updated for stream (${streamName})!`);

        } catch (err: any) {
            // For any other error, rethrow it.
            if (err.name !== "ConsumerNotFoundError") {
                this.logger.error(`Error during setup consumer (${consumerName}):`,
                    (err as Error).message,
                );
                throw err;
            }

            // Create the consumer on the stream.
            await jetstreamManager.consumers.add(streamName, config);

            // Log the creation
            this.logger.info(`Consumer created for stream (${streamName})!`);
        }

        // Retrieve the consumer.
        const consumer = await jetstreamClient.consumers.get(streamName, consumerName);
        return consumer;
    }

    // Private

    private getDefaultConfig(config: Partial<ConsumerConfig>): Partial<ConsumerConfig> {
        // Merge the config with the default config
        return  {
            ack_wait: DEFAULT_ACK_WAIT_MS * 1_000_000,  // How long to wait for an ack
            ack_policy: AckPolicy.Explicit,             // All messages must be acknowledged
            deliver_policy: DeliverPolicy.All,          // All messages must be delivered
            ...config,
        };
    }
}