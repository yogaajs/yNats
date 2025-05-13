import type { IStream, IPayload } from '../types';
import { RetentionPolicy, StorageType, DiscardPolicy } from "@nats-io/jetstream";

import Logger from 'src/classes/logger.class';
import Manager from 'src/classes/manager.class';

// Class
// ===========================================================

export class Stream {
    private readonly _logger: Logger;
    private readonly _connection: Manager;

    private readonly _client: IStream['client'];
    private readonly _config: IStream['config'];

    // Constructor

    constructor({ client, config }: IStream) {
        if (!client) {
            throw new Error("Client is required!");
        }
        if (!config.name) {
            throw new Error("Stream name is required!");
        }

        // Modules
        this._client = client;
        this._logger = new Logger(`[nats][stream][${config.name}]`);
        this._connection = new Manager();

        // Create the stream
        this._config = this.setupStreamConfig(config);
        this.verifyStreamExist();
    }

    // Public

    public get config(): IStream['config'] {
        return this._config;
    }

    public async isReady(): Promise<void> {
        return this._connection.isReady();
    }

    public async getMessage(msgId: string | number): Promise<IPayload['data'] | null> {
        const { _client, _config: { name: streamName } } = this;

        // Wait for the client to be ready
        await _client.clientReady.isReady();

        // Build the subject that corresponds to this id.
        const subject = `${streamName}.${msgId}`;

        // Retrieve the message at the given sequence
        const storedMessage = await _client.jetstreamManager.streams.getMessage(
            streamName, 
            { last_by_subj: subject }
        );

        // Message not found
        if (!storedMessage) { return null; }

        // Decode the message
        const message = storedMessage.json() as IPayload;

        return message.data;
    }
    
    public async getLastMessage(): Promise<IPayload['data'] | null> {
        const { _client, _config: { name: streamName } } = this;

        // Wait for the client to be ready
        await _client.clientReady.isReady();

        // Retrieve stream information to find the last sequence number
        const streamInfo = await _client.jetstreamManager.streams.info(streamName);

        if (!streamInfo.state.last_seq) {
            return null; // No messages in the stream
        }
    
        // Retrieve the message at the last sequence
        const storedMessage = await _client.jetstreamManager.streams.getMessage(
            streamName, 
            { seq: streamInfo.state.last_seq }
        );

        // Message not found
        if (!storedMessage) { 
            throw new Error(`Cant get message with seq: ${streamInfo.state.last_seq}`);
        }

        // Decode the message
        const message = storedMessage.json() as IPayload;

        return message.data;
    }

    public async createStream() {
        const { _client, _logger, _config, _connection } = this;
        try {
            // Wait for the client to be ready
            await _client.isReady();

            // Create the stream with the given configuration.
            await _client.jetstreamManager.streams.add(_config);

            // Log the creation
            _logger.info(`Stream created!`);

        } catch (error: any) {
            _logger.error("Error during stream creation");
            throw error;
        } finally {
            // Set the stream manager as ready
            _connection.setReady();
        }
    }

    public async updateStream() {
        const { _client, _logger, _config, _connection } = this;
        try {
            // Wait for the client to be ready
            await _client.isReady();

            // Retrieve current stream info.
            const info = await _client.jetstreamManager.streams.info(_config.name);
            
            // Merge the current config with the new configuration.
            // New config values in this.streamConfig will overwrite the current config values.
            const updatedConfig = { ...info.config, ..._config };

            // Update the stream with the merged configuration.
            await _client.jetstreamManager.streams.update(_config.name, updatedConfig);

            // Log the update
            _logger.info(`Stream updated!`);

        } catch (error: any) {
            _logger.error("Error during stream update");
            throw error;
        } finally {
            // Set the stream manager as ready
            _connection.setReady();
        }
    }

    public async deleteStream() {
        const { _client, _logger, _config, _connection } = this;
        try {
            // Wait for the client to be ready
            await _client.isReady();

            // Create the stream with the given configuration.
            await _client.jetstreamManager.streams.delete(_config.name);

            // Log the deletion
            _logger.info(`Stream deleted!`);

        } catch (error: any) {
            _logger.error("Error during stream deletion");
            throw error;
        } finally {
            // Set the stream manager as not ready
            _connection.setNotReady();
        }
    }

    // Private

    private setupStreamConfig(config: IStream['config']): IStream['config'] {
        // Merge the config with the default config
        const defaultConfig = {
            retention: RetentionPolicy.Limits,      // Keep messages until acknowledged
            discard: DiscardPolicy.Old,             // Remove old messages when full
            storage: StorageType.Memory,            // Store messages on memory
            max_consumers: 3,                       // Max consumers for this stream
            max_msgs: 100,                          // Max messages in the stream
            max_age: 2 * 60 * 60 * 1_000_000_000,   // 2 hours (nanoseconds)
            max_bytes: 100 * 1024 * 1024,           // 100MB
            ...config,
        };

        // Add the name and subjects to the config (force)
        defaultConfig.name = config.name;
        defaultConfig.subjects = [`${config.name}.>`];

        return defaultConfig;
    }

    private async verifyStreamExist() {
        const { _client, _logger, _config, _connection } = this;
        try {
            // Wait for the client to be ready
            await _client.isReady();

            // Retrieve current stream info.
            const info = await _client.jetstreamManager.streams.info(_config.name);

            // Set the stream manager as ready
            _connection.setReady();

        } catch (error: any) {
            _logger.warn("Stream not found");
        }
    }
}
