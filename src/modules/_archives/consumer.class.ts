import type { IConsumer, IPayload } from '../types';
import type { JsMsg, ConsumerMessages, ConsumeOptions } from '@nats-io/jetstream';
import { DEFAULT_ACK_WAIT_MS } from './consumers.class';
import { AckPolicy, DeliverPolicy, Consumer } from '@nats-io/jetstream';
import { Consumers } from './consumers.class';
import Logger from '@/classes/logger.class';


import { SimpleMutex } from "@nats-io/nats-core/internal";

// Types
// ===========================================================

export type Callback = (subjects: string[], data: IPayload['data'], reply?: (data: Record<string, any>) => void) => Promise<void>;
export type Options = ConsumeOptions;

// Class
// ===========================================================

export class StreamConsumer extends Consumers {
    private readonly config: IConsumer['config'];
    
    private subscribeActive: boolean = false;
    private unsubscribeActive: boolean = false;

    private consumerMessages: ConsumerMessages | null = null;
    private mutex = new SimpleMutex(5);

    // Constructor

    constructor({ client, stream, config }: IConsumer) {
        super({ client, stream });

        // Initialize
        this.config = this.setDefaultConfig(config);
        this.client.clientCleaner.register(() => this.unsubscribe());
    }

    // Public

    public async subscribe(_callback: Callback, _options: Options = {}): Promise<void> {
        if (this.subscribeActive) {
            throw new Error("Subscription is already active!");
        }
        if (this.unsubscribeActive) {
            throw new Error("Unsubscribe is already requested!");
        }

        // Set the flag
        this.subscribeActive = true;

        try {
            // Set the consume options
            const consumeOptions = { max_messages: 3, ..._options };

            // This is the basic pattern for processing messages forever
            while (true) {

                // Ensure client and stream are ready
                await this.client.isReady();
                await this.stream.isReady();

                try {
                    // Get the consumer
                    const consumer = await this.consumerSetup(this.config);

                    // Get the consumer messages
                    this.consumerMessages = await consumer.consume(consumeOptions);

                    // Log the subscribe
                    this.logger.info(`subscription started!`);

                    // Consume messages (infinite loop)
                    await this.consumeMessages(_callback);

                    // Log the unsubscribe
                    this.logger.info(`subscription stopped!`);

                    // If the processing is stop without an error, break the while loop
                    break;
        
                } catch (error) {
                    this.logger.error(`subscription error:`, 
                        (error as Error).message,
                    );
                }

                // Wait for 1 second before retrying
                await new Promise(resolve => setTimeout(resolve, 1_000));
            }

            // Stop the consumer (exit the for loop)
            if (this.consumerMessages) {
                this.consumerMessages.stop();
            }

        } finally {
            // Reset
            this.subscribeActive = false;
            this.consumerMessages = null;
        }
    }

    public async unsubscribe(): Promise<void> {
        if (this.unsubscribeActive) {
            throw new Error("Unsubscribe is already requested!");
        }

        // Request the unsubscribe
        this.unsubscribeActive = true;

        try {
            // Stop the consumer (exit the for loop)
            if (this.consumerMessages) {
                this.consumerMessages.stop();
            }

            // Wait for the subscription to be stopped
            while (this.subscribeActive) {
                this.logger.info(`waiting for consumer to finish processing...`);
                await new Promise(resolve => setTimeout(resolve, 2_000));
            }

        } finally {
            // Reset the status
            this.unsubscribeActive = false;
            this.logger.info(`unsubscribe terminated!`);
        }
    }

    // Private

    private setDefaultConfig(config: IConsumer['config']): IConsumer['config'] {
        // Merge the config with the default config
        return {
            ack_wait: DEFAULT_ACK_WAIT_MS * 1_000_000,  // How long to wait for an ack
            ack_policy: AckPolicy.Explicit,             // All messages must be acknowledged
            deliver_policy: DeliverPolicy.All,          // All messages must be delivered
            ...config,
        };
    }

    protected async consumerSetup2(): Promise<Consumer> {
        // Constants
        const { durable_name: consumerName } = this.config;
        const { name: streamName } = this.stream.config;
        const { jetstreamManager, jetstreamClient } = this.client;

        try {
            // Check if consumer exists.
            const info = await jetstreamManager.consumers.info(streamName, consumerName);

            // Merge the current config with the new configuration.
            // New config values in this.consumerConfig will overwrite the current config values.
            const updatedConfig = { ...info.config, ...this.config };
            updatedConfig.durable_name = consumerName;

            // Update the consumer with the merged configuration.
            await jetstreamManager.consumers.update(streamName, consumerName, updatedConfig);

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
            await jetstreamManager.consumers.add(streamName, this.config);

            // Log the creation
            this.logger.info(`Consumer created for stream (${streamName})!`);
        }

        // Retrieve the consumer.
        const consumer = await jetstreamClient.consumers.get(streamName, consumerName);
        return consumer;
    }

    private createWorkingSignal(_msg: JsMsg): () => void {
        const intervalDelay = Math.floor(DEFAULT_ACK_WAIT_MS * 0.75);

        const workingInterval = setInterval(() => {
            _msg.working();
        }, intervalDelay);

        _msg.working();
        
        return () => 
            clearInterval(workingInterval);
    }

    private async consumeMessages(_callback: Callback): Promise<void> {
        if (!this.consumerMessages) {
            throw new Error("Consumer messages not found!");
        }
        if (this.unsubscribeActive) {
            return; // Unsubscribe requested, exit early
        }

        // Consume messages
        for await (const msg of this.consumerMessages) {
            await this.mutex.lock();

            // Consume the message
            this.consumeMessage(msg, _callback)
                .finally(() => this.mutex.unlock());

            // Unsubscribe requested, break the loop.
            // additional security with .stop() to ensure breaking the loop
            if (this.unsubscribeActive) {
                break;
            }
        }
    }

    private async consumeMessage(msg: JsMsg, callback: Callback): Promise<void> {
        // Constants
        const { logger, client } = this;
        const { name: streamName } = this.stream.config;

        // Create a working signal
        const clearWorkingSignal = this.createWorkingSignal(msg);

        try {
            // Decode the message
            const subjects = msg.subject.split('.');
            const payload = msg.json() as IPayload;

            // Decode the headers
            const uuid = msg.headers?.get('reply-uuid');
            const inbox = msg.headers?.get('reply-inbox');

            // Respond to the request
            const reply = (data: Record<string, any>) => {
                if (inbox && uuid) {
                    client.natsConnection.publish(inbox, JSON.stringify(data), {
                        headers: msg.headers
                    });
                }
            }

            // Process the message
            await callback(subjects, payload, reply);

            // Acknowledge the message
            clearWorkingSignal();

            // ACK to indicate the message has been processed
            msg.ack(); 

        } catch (error) {
            clearWorkingSignal();
            msg.nak();  // NAK to indicate the message has not been processed (failed)

            logger.error(`cannot process message for stream (${streamName}):`, 
                (error as Error).message,
            );
        }
    }
}