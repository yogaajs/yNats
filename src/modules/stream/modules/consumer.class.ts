import type { IPayload } from '../types';
import type { JsMsg, ConsumerMessages, Consumer, ConsumerConfig } from '@nats-io/jetstream';
import type { Client } from "src/core/client.class";
import type { StreamPublisher } from './publisher.class';
import { Common } from '../classes/common.class';
import { getSubject } from '../utils';

// Types
// ===========================================================

export namespace StreamConsumer {
    export type Config = StreamPublisher.Config & {
        streamSubject: NonNullable<ConsumerConfig['filter_subject']>,
        consumerName: NonNullable<ConsumerConfig['durable_name']>,
    };
    export type Options = {
        debug: boolean;
    };
    export type Callback = (subject: string, payload: IPayload) => Promise<void>;
};

// Class
// ===========================================================

export class StreamConsumer extends Common<'consumer'> {
    private readonly options: StreamConsumer.Options;

    private consumer: Consumer | null = null;
    private consumerMessages: ConsumerMessages | null = null;
    private messagesActive: number = 0;

    private subscribeActive: boolean = false;
    private unsubscribeActive: boolean = false;

    // Constructor

    constructor(
        client: Client,
        config: StreamConsumer.Config,
        options?: Partial<StreamConsumer.Options>
    ) {
        super(client, config);

        // Options
        this.options = {
            debug: options?.debug ?? false,
        } satisfies StreamConsumer.Options;

        // Setup
        this.setupStream()
            .then(() => {
                this.setupConsumer()
                    .then(() => {
                        this.manager.setReady();
                    });
            });
    }

    // Public

    public async subscribe(_callback: StreamConsumer.Callback): Promise<void> {
        if (this.subscribeActive) {
            throw new Error("Subscription is already active!");
        }
        if (this.unsubscribeActive) {
            throw new Error("Unsubscribe is already running...");
        }

        // Set
        this.subscribeActive = true;

        try {
            // Ensure client and stream are ready
            await this.isReady();

            // Constants
            const { streamConfig, consumerConfig } = this;

            // Get the consumer
            this.consumer = await this.client.consumer.getPullConsumer(
                streamConfig.name, 
                consumerConfig.durable_name
            );

            // Setup the messages consumer
            await this.setupMessagesConsumer(_callback);

        } finally { 
            // Reset
            this.consumer = null;
            this.subscribeActive = false;
        }
    }

    public async unsubscribe(): Promise<void> {
        if (this.unsubscribeActive) {
            throw new Error("Unsubscribe is already running...");
        }

        // Set
        this.unsubscribeActive = true;

        try {
            // Stop the consumer (exit the for loop)
            if (this.consumerMessages) {
                this.consumerMessages.stop();
            }

            // Wait for all messages to be processed
            while (this.messagesActive > 0) {
                this.logger.info(`waiting for ${this.messagesActive} messages to be processed...`);
                await new Promise(resolve => setTimeout(resolve, 2_000));
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

    // Setup

    private async setupMessagesConsumer(_callback: StreamConsumer.Callback): Promise<void> {
        // This is the basic pattern for processing messages forever
        while (true) {

            // Ensure client and stream are ready
            await this.isReady();

            try {
                // Get the messages consumer
                this.consumerMessages = await this.consumer!.consume({ max_messages: 1 });

                // Log the subscribe
                this.logger.info(`subscription started!`);

                // Consume messages (infinite loop)
                for await (const msg of this.consumerMessages!) {
                    this.messagesActive++;
                    try {
                        await this.consumeMessage(msg, _callback);
                    } finally {
                        this.messagesActive--;
                    }
                }

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

        // Re-stop the consumer (additional security)
        if (this.consumerMessages) {
            this.consumerMessages.stop();
            this.consumerMessages = null;
        }
    }

    private async consumeMessage(msg: JsMsg, callback: StreamConsumer.Callback): Promise<void> {
        // Constants
        const { logger, streamConfig } = this;

        // Create a working signal
        const clearWorkingSignal = this.createWorkingSignal(msg);

        try {
            // Decode the message
            const subject = getSubject(streamConfig.name, msg.subject);
            const payload = msg.json() as IPayload;

            // Process the message
            await callback(subject, payload);
            
            // Acknowledge the message
            clearWorkingSignal();

            // Message has been processed (acknowledge)
            msg.ack(); 

        } catch (error) {
            // Clear the working signal
            clearWorkingSignal();

            // Message has not been processed (not acknowledged)
            msg.nak();

            // Log the error
            logger.error(`cannot process message (${msg.subject}) for stream (${streamConfig.name}):`, 
                (error as Error).message,
            );
        }
    }

    private createWorkingSignal(_msg: JsMsg): () => void {
        const ackWaitMs = this.consumerConfig.ack_wait / 1_000_000;
        const intervalDelay = Math.floor(ackWaitMs * 0.5);

        const workingInterval = setInterval(() => {
            _msg.working();
        }, intervalDelay);
        
        return () => {
            clearInterval(workingInterval);
        }
    }
}