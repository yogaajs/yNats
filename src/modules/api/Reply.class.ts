import type { StreamConfig, PubAck } from "nats";
import type { ProducerConstructor, MessagePayload } from './types';
import { Client } from 'src/client/Client';
import { ReadyPromise } from 'src/utils/ready';
import { sc } from './utils';
import { Logger } from '@/classes/Logger.class';

// Class
// ===========================================================

export class StreamProducer extends Client {
    private readonly logger: Logger;
    private readonly streamName: string;
    private readonly streamConfig: Partial<StreamConfig>;

    private streamReady: ReadyPromise = new ReadyPromise();

    // Constructor

    constructor({ streamConfig }: ProducerConstructor) {
        if (!streamConfig.name) {
            throw new Error("Stream name is required");
        }

        super();

        this.streamName = streamConfig.name;
        this.streamConfig = streamConfig;

        this.logger = new Logger(`[nats][producer][${this.streamName}]`);

        this.init();
    }

    // Public

    public async publish(msgId: string | number, data: Record<string, any>): Promise<PubAck> {
        const timestamp = Date.now(); 
        await this.isClientReady();
        await this.streamReady.isReady();

        const operation = (async () => {
            try {
                const subject = `${this.streamName}.${msgId}`;

                const payload = JSON.stringify({ 
                    timestamp: timestamp, 
                    data: data
                });

                const pubAck = await this.jsc!.publish(subject, sc.encode(payload), {
                    expect: { streamName: this.streamName },
                });

                return pubAck;

            } catch (error) {
                this.logger.error(`Failed to publish message to stream:`, error);
                throw error;
            }
        })();

        return Client.operations.track(operation);
    }

    public async getMessage(msgId: string | number) {
        await this.isClientReady();
        await this.streamReady.isReady();

        const operation = (async () => {
            try {
                // Build the subject that corresponds to this id.
                const subject = `${this.streamName}.${msgId}`;

                // Retrieve the message at the given sequence
                const msg = await this.jsm!.streams.getMessage(this.streamName, { 
                    last_by_subj: subject 
                });

                // Decode the message
                const stringifiedMessage = sc.decode(msg.data);
                const message = JSON.parse(stringifiedMessage) as MessagePayload;
                return message.data;

            } catch (error: any) {
                this.logger.error(`Error retrieving message ${msgId}:`, (error as Error).message);
                throw error;
            }
        })();

        return Client.operations.track(operation);
    }

    public async getLastMessage() {
        await this.isClientReady();
        await this.streamReady.isReady();

        const operation = (async () => {
            try {
                // Retrieve stream information to find the last sequence number
                const streamInfo = await this.jsm!.streams.info(this.streamName);
                const lastSeq = streamInfo.state.last_seq;
            
                if (!lastSeq) {
                    console.log("No messages found in the stream.");
                    return null;
                }
            
                // Retrieve the message at the last sequence
                const storedMessage = await this.jsm!.streams.getMessage(this.streamName, { seq: lastSeq });

                // Decode the message
                const stringifiedMessage = sc.decode(storedMessage.data);
                const message = JSON.parse(stringifiedMessage) as MessagePayload;
                return message.data;

            } catch (error) {
                this.logger.error("Error retrieving last message:", (error as Error).message);
                throw error;
            }
        })();
        return Client.operations.track(operation);
    }

    // Private

    private async init(): Promise<void> {
        await this.isClientReady();
        try {
            // Retrieve current stream info.
            const info = await this.jsm!.streams.info(this.streamName);
            
            // Merge the current config with the new configuration.
            // New config values in this.streamConfig will overwrite the current config values.
            const updatedConfig = {
                ...info.config,
                ...this.streamConfig
            };

            // Update the stream with the merged configuration.
            await this.jsm!.streams.update(this.streamName, updatedConfig);
            this.logger.info(`Stream updated`);

        } catch (err: any) {
            if (err.message && err.message.includes("stream not found")) {
                // If the stream doesn't exist, create it.
                await this.jsm!.streams.add({ name: this.streamName, subjects: this.streamConfig.subjects || [] });
                this.logger.info(`Stream created`);
            } else {
                this.logger.error("Error ensuring stream:", err);
                throw err;
            }
        }

        this.streamReady.setReady();
    }
}
