import type { Client } from "src/core/client.class";
import { StreamConfig, StreamInfo, StreamUpdateConfig, JetStreamApiError } from "@nats-io/jetstream";
import { WithRequired } from '@nats-io/transport-node';
import Logger from '@/classes/logger.class';

// Class
// ===========================================================

export class Streams {
    private readonly _logger: Logger;
    private readonly _client: Client;

    // Constructor

    constructor(client: Client) {
        this._logger = new Logger({ prefix: "[nats][streams]" });
        this._client = client;
    }

    // Public

    public async info(streamName: StreamConfig['name']): Promise<StreamInfo> {
        const { _client } = this;

        // Wait for the client to be ready
        await _client.isReady();

        // Retrieve current stream info.
        const info = await _client.jetstreamManager.streams.info(streamName);
        return info;
    }

    public async create(streamConfig: WithRequired<Partial<StreamConfig>, 'name'>): Promise<StreamInfo> {
        const { _client, _logger } = this;

        // Wait for the client to be ready
        await _client.isReady();

        // Create the stream with the given configuration.
        const info = await _client.jetstreamManager.streams.add(streamConfig);

        // Log the creation
        _logger.info(`Stream "${streamConfig.name}" created!`);

        return info;
    }

    public async update(streamName: StreamConfig['name'], streamConfig: Partial<StreamUpdateConfig>): Promise<StreamInfo> {
        const { _client, _logger } = this;

        // Wait for the client to be ready
        await _client.isReady();

        // Update the stream with the merged configuration.
        const info = await _client.jetstreamManager.streams.update(streamName, streamConfig);

        // Log the deletion
        _logger.info(`Stream "${streamName}" updated!`);

        return info;
    }

    public async delete(streamName: StreamConfig['name']): Promise<boolean> {
        const { _client, _logger } = this;

        // Wait for the client to be ready
        await _client.isReady();

        // Delete the stream
        const deleted = await _client.jetstreamManager.streams.delete(streamName);

        // Log the deletion
        _logger.info(`Stream "${streamName}" deleted!`);

        return deleted;
    }
}
