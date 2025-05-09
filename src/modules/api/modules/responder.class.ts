import type { JsMsg, ConsumerMessages, Consumer } from '@nats-io/jetstream';
import type { Client } from "src/core/client.class";
import type { StreamRequester } from './requester.class';
import { Common } from '../classes/common.class';
import { getSubject, getHeader } from '../utils';
import Mutex from '@/classes/mutex.class';

// Types
// ===========================================================

export namespace StreamResponder {
    export type Config = StreamRequester.Config & {
        consumerName: string,
        filterSubject: string,
    };
    export type Options = {
        maxConcurrent: number,
        debug: boolean;
    };
    export type Payload = {
        timestamp: number;
        data: any;
    };
    export type Callback<T> = (subject: string, payload: Payload) => Promise<T>;
};

// Class
// ===========================================================

export class StreamResponder extends Common<'responder'> {
    private readonly mutex: Mutex;
    private readonly options: StreamResponder.Options;

    private consumer: Consumer | null = null;
    private consumerMessages: ConsumerMessages | null = null;

    private subscribeActive: boolean = false;
    private unsubscribeActive: boolean = false;

    // Constructor

    constructor(
        client: Client,
        config: StreamResponder.Config,
        options?: Partial<StreamResponder.Options>
    ) {
        super(client, config);

        // Options
        this.options = {
            maxConcurrent: options?.maxConcurrent ?? 1,
            debug: options?.debug ?? false,
        } satisfies StreamResponder.Options;

        // Setup
        this.mutex = new Mutex(this.options.maxConcurrent);

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

    public async subscribe<T>(_callback: StreamResponder.Callback<T>): Promise<void> {
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
            while (this.mutex.waitingCount > 0) {
                this.logger.info(`waiting for ${this.mutex.waitingCount} messages to be processed...`);
                await new Promise(resolve => setTimeout(resolve, 2_000));
            }

            // Wait for all messages to finish
            while (this.mutex.activeCount > 0) {
                this.logger.info(`waiting for ${this.mutex.activeCount} messages to finish...`);
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

    private async setupMessagesConsumer<T>(_callback: StreamResponder.Callback<T>): Promise<void> {
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
                    await this.mutex.lock();
                    this.consumeMessage(msg, _callback)
                        .finally(() => {
                            this.mutex.unlock();
                        });
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

    private async consumeMessage<T>(msg: JsMsg, callback: StreamResponder.Callback<T>): Promise<void> {
        // Constants
        const { logger, client, streamConfig } = this;

        // Create a working signal
        const clearWorkingSignal = this.createWorkingSignal(msg);

        try {
            // Decode the message
            const inbox = getHeader(msg.headers!);
            const subject = getSubject(streamConfig.name, msg.subject);
            const payload = msg.json() as StreamResponder.Payload;

            // Process the message
            const result = await callback(subject, payload);
            const payloadResult = JSON.stringify({ timestamp: Date.now(), data: result });
            
            // Acknowledge the message
            clearWorkingSignal();

            // Respond to the request
            client.natsConnection.publish(inbox, payloadResult, {
                headers: msg.headers
            });

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