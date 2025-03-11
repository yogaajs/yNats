import type { StreamConfig, PubAck, ConsumerMessages, ConsumerConfig, JsMsg, Consumer } from "nats";
import type { ProducerConstructor, MessagePayload, ConsumerConstructor } from '../types';
import { Client } from 'src/client/Client';
import { Readiness } from 'src/classes/Readiness.class';
import { sc } from '../utils';
import { Logger } from '@/classes/Logger.class';
import { StaticShutdownManager } from '@/classes/Shutdown.class';
import { AckPolicy, DeliverPolicy } from 'nats';

// Class
// ===========================================================

export class StreamConsumer extends Client {
    private readonly _streamName: string;
    private readonly _consumerName: string;
    private readonly _consumerConfig: ConsumerConstructor['consumerConfig'];
    private readonly _consumerLogger: Logger;
    private readonly _consumerReady: Readiness = new Readiness();

    private cm: ConsumerMessages | null = null;

    // Constructor

    constructor({ streamName, consumerName, consumerConfig }: ConsumerConstructor) {
        if (!streamName) {
            throw new Error("Stream name is required");
        }
        if (!consumerName) {
            throw new Error("Consumer name is required");
        }

        super();

        this._streamName = streamName;
        this._consumerName = consumerName;
        this._consumerConfig = consumerConfig;

        this._consumerLogger = new Logger(`[jetstream][consumer][${consumerName}]`);
    }

    // Getters

    public get consumerReady(): Readiness {
        return this._consumerReady;
    }


    public get logger(): Logger {
        return this._consumerLogger;
    }

    // Public

    public async subscribe(handler: (data: any) => Promise<void>): Promise<void> {
        // Ensure client is ready
        await this.clientReady.isReady();

        // Retrieve or create the consumer
        const consumer = await this._createOrUpdateConsumer();
        StaticShutdownManager.register(() => this.unsubscribe());

        // Using the new API, get an async iterator for messages.
        // Note: The consumer’s configuration (subject filters, etc.) should ensure
        // that only messages of interest are delivered.
        this.cm = await consumer.consume({ 
            max_messages: 5,
            callback: async (msg: JsMsg) => {
                try {
                    const stringifiedMessage = sc.decode(msg.data);
                    const message = JSON.parse(stringifiedMessage) as MessagePayload;

                    msg.working(); // Notify that the message is being processed
                    await handler(message);
                    msg.ack(); // ACK to indicate the message has been processed

                } catch (error) {
                    this.logger.error("Error processing message:", error);
                    msg.nak(); // NAK to indicate the message should be retried later
                }
            },
        });
    }

    public async unsubscribe(): Promise<void> {
        if (this.cm) {
            this.cm.stop();
            this.logger.info(`Consumer unsubscribed.`);
        }
    }

    // Private

    private async _createOrUpdateConsumer(): Promise<Consumer> {
        try {
            // Check if consumer exists.
            const info = await this.jsm!.consumers.info(this._streamName, this._consumerName);

            // Merge the current config with the new configuration.
            // New config values in this.consumerConfig will overwrite the current config values.
            const updatedConfig = { ...info.config, ...this._consumerConfig };

            // Update the consumer with the merged configuration.
            await this.jsm!.consumers.update(this._streamName, this._consumerName, updatedConfig);

            // Log the update
            this.logger.info(`Consumer updated!`);

        } catch (err: any) {

            // Check if error indicates that the consumer was not found.
            if (err.message && err.message.includes("consumer not found")) {

                // Create the consumer on the stream.
                await this.jsm!.consumers.add(this._streamName, {
                    durable_name: this._consumerName,
                    ack_policy: AckPolicy.Explicit,
                    deliver_policy: DeliverPolicy.All,
                    ...this._consumerConfig,
                });

                // Log the creation
                this.logger.info(`Consumer created!`);

            } else {

                // For any other error, rethrow it.
                this.logger.error("Error during consumer creation or update:", err);
                throw err;
            }
        }

        // Retrieve the consumer.
        const consumer = await this.jsc!.consumers.get(this._streamName, this._consumerName);
        return consumer;
    }
}
