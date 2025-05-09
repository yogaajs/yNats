import type { IStream, IPayload } from '../types';
import type { WithRequired } from '@nats-io/transport-node';
import type { StreamConfig } from "@nats-io/jetstream";
import { RetentionPolicy, StorageType, DiscardPolicy } from "@nats-io/jetstream";

import Logger from '@/classes/logger.class';
import Manager from '@/classes/manager.class';

// Class
// ===========================================================

export class Stream {
    private readonly client: IStream['client'];
    private readonly logger: Logger;

    public readonly streamConfig: WithRequired<Partial<StreamConfig>, 'name' | 'subjects'>;
    public readonly streamManager: Manager;

    // Constructor

    constructor({ client, options }: IStream) {
        if (!client) {
            throw new Error("Client is required!");
        }
        if (!options.name) {
            throw new Error("Stream name is required!");
        }

        // Private
        this.client = client;
        this.logger = new Logger(`[nats][stream][${options.name}]`);

        // Public
        this.streamConfig = this.setupStreamConfig(options);
        this.streamManager = new Manager();

        // Configure the stream
        this.setupStream(this.streamConfig);
    }

    // Public

    public async getMessage(msgId: string | number): Promise<IPayload['data'] | null> {
        const { client, logger, streamConfig } = this;

        try {
            // Wait for the client to be ready
            await client.clientReady.isReady();

            // Build the subject that corresponds to this id.
            const subject = `${streamConfig.name}.${msgId}`;
    
            // Retrieve the message at the given sequence
            const storedMessage = await client.jetstreamManager.streams.getMessage(
                streamConfig.name, 
                { last_by_subj: subject }
            );
    
            // Message not found
            if (!storedMessage) { return null; }
    
            // Decode the message
            const message = storedMessage.json() as IPayload;
            return message.data;
    
        } catch (error: any) {
            logger.error(`Error retrieving message ${msgId} in stream (${streamConfig.name}):`, 
                (error as Error).message
            );
            throw error;
        }
    }
    
    public async getLastMessage(): Promise<IPayload['data'] | null> {
        const { client, logger, streamConfig } = this;

        try {
            // Wait for the client to be ready
            await client.clientReady.isReady();

            // Retrieve stream information to find the last sequence number
            const streamInfo = await client.jetstreamManager.streams.info(streamConfig.name);
    
            if (!streamInfo.state.last_seq) {
                return null; // No messages in the stream
            }
        
            // Retrieve the message at the last sequence
            const storedMessage = await client.jetstreamManager.streams.getMessage(
                streamConfig.name, 
                { seq: streamInfo.state.last_seq }
            );
    
            // Message not found
            if (!storedMessage) { 
                throw new Error(`Cant get message with seq: ${streamInfo.state.last_seq}`);
            }
    
            // Decode the message
            const message = storedMessage.json() as IPayload;
            return message.data;
    
        } catch (error: any) {
            logger.error(`Error retrieving last message in stream (${streamConfig.name}):`, 
                (error as Error).message
            );
            throw error;
        }
    }

    // Private

    private setupStreamConfig(options: IStream['options']) {
        // Merge the config with the default config
        const defaultConfig: Partial<StreamConfig> = {
            retention: RetentionPolicy.Limits,      // Keep messages within the limits (size/age/msgs)
            discard: DiscardPolicy.Old,             // Remove old messages when full
            storage: StorageType.File,              // Store messages on disk
            max_consumers: 10,                      // Max consumers for this stream
            max_msgs: 1_000,                        // Max messages in the stream
            max_age: 24 * 60 * 60 * 1_000_000_000,  // 24 hours (nanoseconds)
            max_bytes: 1024 * 1024 * 1024,          // 1GB
            ...options.config,
        };

        // Add the name and subjects to the config
        defaultConfig.name = options.name;
        defaultConfig.subjects = [`${options.name}.*`];

        return defaultConfig as WithRequired<Partial<StreamConfig>, 'name' | 'subjects'>;
    }

    private async setupStream(streamConfig: WithRequired<Partial<StreamConfig>, 'name' | 'subjects'>) {
        const { client, logger, streamManager } = this;

        try {
            // Wait for the client to be ready
            await client.clientManager.isReady();

            // Retrieve current stream info.
            const info = await client.jetstreamManager.streams.info(streamConfig.name);
            
            // Merge the current config with the new configuration.
            // New config values in this.streamConfig will overwrite the current config values.
            const updatedConfig = { ...info.config, ...streamConfig };

            // Update the stream with the merged configuration.
            await client.jetstreamManager.streams.update(streamConfig.name, updatedConfig);

            // Log the update
            logger.info(`Stream updated!`);

        } catch (error: any) {
            if (error.name !== "StreamNotFoundError") {
                // If the stream exists, but there was an error during the update, throw the error.
                logger.error("Error during stream creation or update:", error);
                throw error;
            }

            // Create the stream with the given configuration.
            await client.jetstreamManager.streams.add(streamConfig);

            // Log the creation
            logger.info(`Stream created!`);
        }
        
        // Set the stream manager as ready
        streamManager.setReady();
    }
}
