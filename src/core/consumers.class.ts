import type { Client } from "src/core/client.class";
import type { Consumer, ConsumerInfo, ConsumerConfig, PushConsumer } from '@nats-io/jetstream';
import Logger from '@/classes/logger.class';

// Class
// ===========================================================

export class Consumers {
    private readonly _logger: Logger;
    private readonly _client: Client;

    // Constructor

    constructor(client: Client) {
        this._logger = new Logger(`[nats][consumers]`);
        this._client = client;
    }

    // Public

    public async list(streamName: string): Promise<ConsumerInfo[]> {
        // Wait for client to be ready
        await this._client.isReady();

        // Constants
        const { consumers } = this._client.jetstreamManager;

        // Get the list of consumers
        return await consumers.list(streamName).next();
    }

    public async info(streamName: string, consumerName: string): Promise<ConsumerInfo> {
        // Wait for client to be ready
        await this._client.isReady();

        // Constants
        const { consumers } = this._client.jetstreamManager;

        // Retrieve the consumer info
        return await consumers.info(streamName, consumerName);
    }

    public async create(streamName: string, config: Partial<ConsumerConfig>): Promise<ConsumerInfo> {
        // Wait for resources to be ready
        await this._client.isReady();

        // Constants
        const { jetstreamManager } = this._client;

        // Retrieve the consumer.
        const info = await jetstreamManager.consumers.add(streamName, config);

        // Log the creation
        this._logger.info(`Consumer "${info.config.durable_name}" created!`);

        return info;
    }

    public async delete(streamName: string, consumerName: string): Promise<boolean> {
        // Wait for resources to be ready
        await this._client.isReady();

        // Constants
        const { jetstreamManager } = this._client;

        // Retrieve the consumer.
        const isDeleted = await jetstreamManager.consumers.delete(streamName, consumerName);

        // Log the creation
        this._logger.info(`Consumer "${consumerName}" deleted!`);

        return isDeleted;
    }

    public async getPullConsumer(streamName: string, consumerName: string): Promise<Consumer> {
        // Wait for client to be ready
        await this._client.isReady();

        // Constants
        const { jetstreamClient, jetstreamManager } = this._client;

        // Retrieve the consumer.
        const info = await jetstreamManager.consumers.info(streamName, consumerName);

        if (!info.config.durable_name) {
            throw new Error("Consumer is ephemeral");
        }

        if (info.config.deliver_subject) {
            throw new Error("Consumer is push consumer");
        }

        return await jetstreamClient.consumers.get(streamName, consumerName);
    }

    public async getPushConsumer(streamName: string, consumerName: string): Promise<PushConsumer> {
        // Wait for resources to be ready
        await this._client.isReady();

        // Constants
        const { jetstreamClient, jetstreamManager } = this._client;

        // Retrieve the consumer.
        const info = await jetstreamManager.consumers.info(streamName, consumerName);

        if (!info.config.durable_name) {
            throw new Error("Consumer is ephemeral");
        }

        if (!info.config.deliver_subject) {
            throw new Error("Consumer is pull consumer");
        }

        return await jetstreamClient.consumers.getPushConsumer(streamName, consumerName);
    }
}