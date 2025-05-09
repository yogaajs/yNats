import type { IConsumer, IPayload } from '../types';
import type { Consumer, JsMsg, ConsumerMessages, ConsumerConfig, ConsumeOptions } from '@nats-io/jetstream';
import { AckPolicy, DeliverPolicy } from '@nats-io/jetstream';

import Logger from '@/classes/logger.class';

// Constants
// ===========================================================

const DEFAULT_ACK_WAIT_MS: number = 10 * 1_000; // 10s

// Types
// ===========================================================

export type Callback = (data: IPayload['data'], reply: (data: Record<string, any>) => void) => Promise<void>;
export type Options = ConsumeOptions;

// Class
// ===========================================================

export class StreamResponder {
    private readonly client: IConsumer['client'];
    private readonly stream: IConsumer['stream'];
    private readonly logger: Logger;

    private readonly consumerConfig: IConsumer['config'];

    private subscribeActive: boolean = false;
    private unsubscribeActive: boolean = false;

    private consumerMessages: ConsumerMessages | null = null;

    // Constructor

    constructor({ client, stream, config }: IConsumer) {
        if (!client) {
            throw new Error("Client is required!");
        }
        if (!stream) {
            throw new Error("Stream is required!");
        }
        if (!config.durable_name) {
            throw new Error("Consumer name is required!");
        }

        // Private
        this.client = client;
        this.stream = stream;
        this.logger = new Logger(`[nats][consumer][${config.durable_name}]`);

        // Public
        this.consumerConfig = this.setupConsumerConfig(config);

        // Register the cleanup
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

        // Request the subscribe
        this.subscribeActive = true;

        // Constants
        const { logger } = this;

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
                    const consumer = await this.setupConsumer();

                    // Get the consumer messages
                    this.consumerMessages = await consumer.consume(consumeOptions);

                    // Log the subscribe
                    logger.info(`subscription started!`);

                    // Consume messages (infinite loop)
                    await this.consumeMessages(_callback);

                    // Log the unsubscribe
                    logger.info(`subscription stopped!`);

                    // If the processing is stop without an error, break the while loop
                    break;
        
                } catch (error) {
                    logger.error(`subscription error:`, 
                        (error as Error).message,
                    );
                }

                // Wait for 1 second before retrying
                await new Promise(resolve => setTimeout(resolve, 1_000));
            }
        } finally {
            // Reset the status
            this.subscribeActive = false;
        }
    }

    public async unsubscribe(): Promise<void> {
        if (this.unsubscribeActive) {
            throw new Error("Unsubscribe is already requested!");
        }

        // Request the unsubscribe
        this.unsubscribeActive = true;

        // Constants
        const { logger } = this;

        try {
            // Stop the consumer (exit the for loop)
            if (this.consumerMessages) {
                this.consumerMessages.stop();
            }

            // Wait for the subscription to be stopped
            while (this.subscribeActive) {
                logger.info(`waiting for consumer to finish processing...`);
                await new Promise(resolve => setTimeout(resolve, 2_000));
            }

        } finally {
            // Reset the status
            this.unsubscribeActive = false;
            logger.info(`unsubscribe terminated!`);
        }
    }

    // Private

    private setupConsumerConfig(config: IConsumer['config']): IConsumer['config'] {
        // Merge the config with the default config
        const defaultConfig = {
            ack_policy: AckPolicy.Explicit,
            ack_wait: DEFAULT_ACK_WAIT_MS * 1_000_000, // ms * 1000000ns
            deliver_policy: DeliverPolicy.All,
            ...config,
        };

        // Add the name to the config
        defaultConfig.durable_name = config.durable_name;

        return defaultConfig;
    }

    private createWorkingSignal(_msg: JsMsg): () => void {
        const intervalDelay = DEFAULT_ACK_WAIT_MS / 2;

        const workingInterval = setInterval(() => {
            _msg.working();
        }, intervalDelay);

        _msg.working();
        
        return () => clearInterval(workingInterval);
    }

    private async setupConsumer(): Promise<Consumer> {
        // Constants
        const { name: streamName } = this.stream.config;
        const { durable_name: consumerName } = this.consumerConfig;
        const { jetstreamManager, jetstreamClient } = this.client;

        try {
            // Check if consumer exists.
            const info = await jetstreamManager.consumers.info(streamName, consumerName);

            // Merge the current config with the new configuration.
            // New config values in this.consumerConfig will overwrite the current config values.
            const updatedConfig = { ...info.config, ...this.consumerConfig };
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
            await jetstreamManager.consumers.add(streamName, this.consumerConfig);

            // Log the creation
            this.logger.info(`Consumer created for stream (${streamName})!`);
        }

        // Retrieve the consumer.
        const consumer = await jetstreamClient.consumers.get(streamName, consumerName);
        return consumer;
    }

    private async consumeMessages(_callback: Callback): Promise<void> {
        if (!this.consumerMessages) {
            throw new Error("Consumer messages not found!");
        }
        if (this.unsubscribeActive) {
            return; // Unsubscribe requested, exit early
        }

        // Constants
        const { logger, client } = this;
        const { name: streamName } = this.stream.config;

        // Consume messages
        for await (const msg of this.consumerMessages) {
            const clearWorkingSignal = this.createWorkingSignal(msg);
            try {
                // Decode the message
                const action = msg.subject.split(".")[1];
                const payload = msg.json() as IPayload & { inbox?: string };

                // Respond to the request
                const reply = (data: Record<string, any>) => {
                    if (message?.inbox) {
                        client.natsConnection.publish(message.inbox, JSON.stringify(data));
                    }
                }

                // Process the message
                await _callback(payload, action, reply);

                // Acknowledge the message
                clearWorkingSignal();

                msg.ack();  // ACK to indicate the message has been processed

            } catch (error) {
                clearWorkingSignal();
                msg.nak();  // NAK to indicate the message has not been processed (failed)

                logger.error(`cannot process message for stream (${streamName}):`, 
                    (error as Error).message,
                );
            }

            // Unsubscribe requested, break the loop.
            // additional security with .stop() to ensure breaking the loop
            if (this.unsubscribeActive) {
                break;
            }
        }
    }
}