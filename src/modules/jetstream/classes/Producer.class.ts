import type { PubAck } from "@nats-io/jetstream";
import type { ProducerConstructor } from '../types';
import { RetentionPolicy, StorageType, DiscardPolicy } from "@nats-io/jetstream";
import { StreamCommon } from './Common.class';

// Class
// ===========================================================

export class StreamProducer extends StreamCommon {
    private readonly _streamConfig: ProducerConstructor['streamConfig'];

    // Constructor

    constructor({ streamConfig }: ProducerConstructor) {
        if (!streamConfig.name) {
            throw new Error("Stream name is required");
        }
        if (!streamConfig.subjects || streamConfig.subjects.length === 0) {
            throw new Error("Stream subjects are required");
        }

        super("producer", streamConfig.name);

        this._streamConfig = {
            retention: RetentionPolicy.Limits,      // Keep messages within the limits (size/age/msgs)
            discard: DiscardPolicy.Old,             // Remove old messages when full
            storage: StorageType.File,              // Store messages on disk
            max_consumers: 10,                      // Max consumers for this stream
            max_msgs: 1_000,                        // Max messages in the stream
            max_age: 24 * 60 * 60 * 1_000_000_000,  // 24 hours (nanoseconds)
            max_bytes: 1024 * 1024 * 1024,          // 1GB
            ...streamConfig
        };

        this._createOrUpdateStream();
    }

    // Public

    public async publish(msgId: string | number, data: Record<string, any>): Promise<PubAck> {
        const timestamp = Date.now(); 

        await super.clientReady.isReady();
        await this.streamReady.isReady();

        const clearOperation = super.clientOperator.add("producer");
        
        try {
            const subject = `${this.streamName}.${msgId}`;
            const payload = JSON.stringify({ timestamp, data });

            const pubAck = await super.jetstreamClient.publish(subject, payload, {
                expect: { streamName: this.streamName },
            });

            return pubAck;

        } catch (error) {
            this.streamLogger.error(`Failed to publish message to stream:`, error);
            throw error;

        } finally {
            console.log("publish", "done");
            clearOperation();
        }
    }

    // Private

    private async _createOrUpdateStream(): Promise<void> {
        await super.clientReady.isReady();
        try {
            // Retrieve current stream info.
            const info = await super.jetstreamManager.streams.info(this.streamName);
            
            // Merge the current config with the new configuration.
            // New config values in this.streamConfig will overwrite the current config values.
            const updatedConfig = { ...info.config, ...this._streamConfig };

            // Update the stream with the merged configuration.
            await super.jetstreamManager.streams.update(this.streamName, updatedConfig);

            // Log the update
            this.streamLogger.info(`Stream updated!`);

        } catch (err: any) {

            // If the stream doesn't exist, create it.
            if (err.name === "StreamNotFoundError") {

                // Create the stream with the given configuration.  
                await super.jetstreamManager.streams.add(this._streamConfig);

                // Log the creation
                this.streamLogger.info(`Stream created!`);

            } else {

                // If the stream exists, but there was an error during the update, throw the error.
                this.streamLogger.error("Error during stream creation or update:", err);
                throw err;
            }
        }

        this.streamReady.setReady();
    }
}
