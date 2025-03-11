import type { StreamConfig, PubAck } from "nats";
import type { ProducerConstructor } from './types';
import { RetentionPolicy, StorageType, DiscardPolicy } from "nats";
import { Client } from 'src/client/Client';
import { Readiness } from 'src/classes/Readiness.class';
import { sc } from './utils';
import { Logger } from '@/classes/Logger.class';

import { getMessage } from './extensions/getMessage';
import { getLastMessage } from './extensions/getLastMessage';


// Class
// ===========================================================

export class StreamProducer extends Client {
    private readonly _streamName: string;
    private readonly _streamConfig: Partial<StreamConfig>;
    private readonly _streamLogger: Logger;
    private readonly _streamReady: Readiness = new Readiness();

    // Constructor

    constructor({ streamConfig }: ProducerConstructor) {
        if (!streamConfig.name) {
            throw new Error("Stream name is required");
        }
        if (!streamConfig.subjects || streamConfig.subjects.length === 0) {
            throw new Error("Stream subjects are required");
        }

        super();

        this._streamName = streamConfig.name;

        this._streamConfig = {
            retention: RetentionPolicy.Limits, // Keep messages within the limits (size/age/msgs)
            discard: DiscardPolicy.Old, // Remove old messages when full
            storage: StorageType.File, // Store messages on disk
            max_consumers: 10, // Max consumers for this stream
            max_msgs: 1_000, // Max messages in the stream
            max_age: 12 * 60 * 60 * 1_000_000_000, // 12 hours (nanoseconds)
            max_bytes: 1024 * 1024 * 1024, // 1GB
            ...streamConfig
        };

        this._streamLogger = new Logger(`[jetstream][producer][${streamConfig.name}]`);
        this._createOrUpdateStream();
    }

    // Getters

    public get streamReady(): Readiness {
        return this._streamReady;
    }

    public get logger(): Logger {
        return this._streamLogger;
    }

    // Public

    public async getMessage(msgId: string | number) {
        await this.clientReady.isReady();
        await this.streamReady.isReady();
        return getMessage({
            logger: this._streamLogger,
            jsm: this.jsm,
            streamName: this._streamName,
            msgId
        });
    }

    public async getLastMessage() {
        await this.clientReady.isReady();
        await this.streamReady.isReady();
        return getLastMessage({
            logger: this._streamLogger,
            jsm: this.jsm,
            streamName: this._streamName
        });
    }

    public async publish(msgId: string | number, data: Record<string, any>): Promise<PubAck> {
        const timestamp = Date.now(); 

        await this.clientReady.isReady();
        await this.streamReady.isReady();

        try {
            const subject = `${this._streamName}.${msgId}`;

            const payload = JSON.stringify({ 
                timestamp: timestamp, 
                data: data
            });

            const pubAck = await this.jsc!.publish(subject, sc.encode(payload), {
                expect: { streamName: this._streamName },
            });

            return pubAck;

        } catch (error) {
            this._streamLogger.error(`Failed to publish message to stream:`, error);
            throw error;
        }
    }

    // Private

    private async _createOrUpdateStream(): Promise<void> {
        await super.clientReady.isReady();
        try {
            // Retrieve current stream info.
            const info = await super.jsm.streams.info(this._streamName);
            
            // Merge the current config with the new configuration.
            // New config values in this.streamConfig will overwrite the current config values.
            const updatedConfig = { ...info.config, ...this._streamConfig };

            // Update the stream with the merged configuration.
            await super.jsm.streams.update(this._streamName, updatedConfig);

            // Log the update
            this._streamLogger.info(`Stream updated!`);

        } catch (err: any) {

            // If the stream doesn't exist, create it.
            if (err.message && err.message.includes("stream not found")) {

                // Create the stream with the given configuration.
                await super.jsm.streams.add(this._streamConfig);

                // Log the creation
                this._streamLogger.info(`Stream created!`);

            } else {

                // If the stream exists, but there was an error during the update, throw the error.
                this._streamLogger.error("Error during stream creation or update:", err);
                throw err;
            }
        }

        this.streamReady.setReady();
    }
}
