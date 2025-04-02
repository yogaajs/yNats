import type { ConsumerMessages, JsMsg, Consumer } from '@nats-io/jetstream';
import type { MessagePayload, ConsumerConstructor } from '../types';
import { AckPolicy, DeliverPolicy } from '@nats-io/jetstream';

import { StreamCommon } from './Common.class';

// Class
// ===========================================================

export class StreamConsumer extends StreamCommon {
    private readonly _consumerName: ConsumerConstructor['consumerName'];
    private readonly _consumerConfig: ConsumerConstructor['consumerConfig'];

    private _messagesConsumer: ConsumerMessages | null = null;
    private _unsubscribeConsumer: boolean = false;

    // Constructor

    constructor({ streamName, consumerName, consumerConfig }: ConsumerConstructor) {
        if (!streamName) {
            throw new Error("Stream name is required");
        }
        if (!consumerName) {
            throw new Error("Consumer name is required");
        }

        super("consumer", streamName);

        this._consumerName = consumerName;
        this._consumerConfig = consumerConfig;
    }

    // Consumer

    public async subscribe(handler: (data: any) => Promise<void>): Promise<void> {
        await super.clientReady.isReady();

        // Retrieve or create the consumer
        const consumer = await this._createOrUpdateConsumer();
        this._unsubscribeConsumer = false;

        // This is the basic pattern for processing messages forever
        while (true) {
            // Get the messages consumer
            this._messagesConsumer = await consumer.consume({ max_messages: 5 });
            try {
                // Consume messages
                for await (const msg of this._messagesConsumer!) {
                    const clearOperation = super.clientOperator.add("consumer");
                    const clearWorkingSignal = this._workingSignal(msg);
                    try {
                        // Decode the message
                        const message = msg.json() as MessagePayload;

                        // Process the message
                        await handler(message);
                        
                        // Acknowledge the message
                        clearWorkingSignal();
                        msg.ack(); 
    
                    } catch (error) {
                        clearWorkingSignal();
                        msg.nak(); // NAK to indicate the message has not been processed
                        this.streamLogger.error(`${this._consumerName} cannot process message:`, { msg, error });
    
                    } finally {
                        clearOperation();
                        if (this._unsubscribeConsumer) {
                            break;
                        }
                    }
                }
            } catch (err: any) {
                this.streamLogger.error(`${this._consumerName} consume failed:`, err);

            } finally {
                if (this._unsubscribeConsumer) {
                    break;
                }
                await new Promise(resolve => setTimeout(resolve, 500));
            }
        }
    }

    public async unsubscribe(): Promise<void> {
        // Close consumer subscription (stop receiving messages)
        this._unsubscribeConsumer = true;
        this.streamLogger.info(`Consumer unsubscribed.`);
    }

    // Private

    private async _createOrUpdateConsumer(): Promise<Consumer> {
        try {
            // Check if consumer exists.
            const info = await super.jetstreamManager.consumers.info(this.streamName, this._consumerName);

            // Merge the current config with the new configuration.
            // New config values in this.consumerConfig will overwrite the current config values.
            const updatedConfig = { ...info.config, ...this._consumerConfig };

            // Update the consumer with the merged configuration.
            await super.jetstreamManager.consumers.update(this.streamName, this._consumerName, updatedConfig);

            // Log the update
            this.streamLogger.info(`Consumer ${this._consumerName} updated!`);

        } catch (err: any) {

            // Check if error indicates that the consumer was not found.
            if (err.message && err.message.includes("consumer not found")) {

                // Create the consumer on the stream.
                await super.jetstreamManager.consumers.add(this.streamName, {
                    durable_name: this._consumerName,
                    ack_policy: AckPolicy.Explicit,
                    deliver_policy: DeliverPolicy.All,
                    ...this._consumerConfig,
                });

                // Log the creation
                this.streamLogger.info(`Consumer ${this._consumerName} created!`);

            } else {

                // For any other error, rethrow it.
                this.streamLogger.error(`Error during consumer ${this._consumerName} (creation or update):`, err);
                throw err;
            }
        }

        // Set the stream ready.
        this.streamReady.setReady();

        // Retrieve the consumer.
        const consumer = await super.jetstreamClient.consumers.get(this.streamName, this._consumerName);
        return consumer;
    }

    private _workingSignal(msg: JsMsg): () => void {
        const workingInterval = setInterval(() => {
            msg.working();
        }, 5_000);

        msg.working();
        
        return () => clearInterval(workingInterval);
    }
}
