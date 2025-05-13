import type { JsMsg, ConsumerMessages, Consumer } from '@nats-io/jetstream';
import type { Client } from "src/core/client.class";
import { getSubject, getHeader } from '../utils';
import { consumerConfig } from '../config';
import ApiBase, { type API } from '../classes/common.class';
import { compress, decompress } from '@/utils/snappy.utils';

// Types
// ===========================================================

export namespace ApiResponder {
    export type Config = {
        streamName: string,
        consumerName: string,
        //consumerType: 'requests' | 'responses',
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
    export type Callback<T> = 
        (subject: string, payload: Payload) => Promise<T>;
};

// Class
// ===========================================================

export class ApiResponder extends ApiBase {
    private readonly config: API.ConsumerConfig;
    private readonly options: Omit<ApiResponder.Options, 'maxConcurrent' | 'debug'>;

    private consumer: Consumer | null = null;
    private consumerMessages: ConsumerMessages | null = null;
    private subscribeActive: boolean = false;
    private unsubscribeActive: boolean = false;

    // Constructor

    constructor(
        client: Client,
        config: ApiResponder.Config,
        options?: Partial<ApiResponder.Options>
    ) {
        super(client, {
            classType: 'responder',
            streamName: config.streamName,
            maxConcurrent: options?.maxConcurrent,
            debug: options?.debug,
        });

        // Options
        this.options = {
        } satisfies Omit<ApiResponder.Options, 'maxConcurrent' | 'debug'>;

        // Config
        this.config = consumerConfig({
            streamName: config.streamName,
            consumerName: config.consumerName,
            filterSubject: config.filterSubject,
        }) satisfies API.ConsumerConfig;

        // Setup
        this.setupConsumer(this.config)
            .then(() => this.manager.setReady());
    }

    // Public

    public async subscribe<T>(_callback: ApiResponder.Callback<T>): Promise<void> {
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
            const { streamName, config } = this;

            // Get the consumer
            this.consumer = await this.client.consumer.getPullConsumer(
                streamName, 
                config.durable_name
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

    private async setupMessagesConsumer<T>(_callback: ApiResponder.Callback<T>): Promise<void> {
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

                    // Lock the mutex
                    this.mutex.lock()
                        .then(() => {
                            // Process the message
                            this.consumeMessage(msg, _callback)
                                .finally(() => {
                                    // Unlock the mutex
                                    this.mutex.unlock();
                                });
                        });
                    
                    // Log
                    this.logger.debug(
                        `active: ${this.mutex.activeCount}`,
                        `waiting: ${this.mutex.waitingCount}`,
                    );
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

    private async consumeMessage<T>(msg: JsMsg, callback: ApiResponder.Callback<T>): Promise<void> {
        // Constants
        const { logger, client, streamName, config } = this;

        // Create a working signal
        const clearWorkingSignal = this.createWorkingSignal(msg);

        try {
            // Decode the message
            const inbox = getHeader(msg.headers!);
            const subject = getSubject(streamName, msg.subject);
            const payload = msg.json() as ApiResponder.Payload;

            // Process the message
            const result = await callback(subject, payload);
            const payloadResult = { timestamp: Date.now(), data: result };
            const payloadResultCompressed = await compress(payloadResult, true);
            
            // Acknowledge the message
            clearWorkingSignal();

            // Respond to the request
            client.natsConnection.publish(inbox, payloadResultCompressed, {
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
            logger.error(`cannot process message (${msg.subject}) for stream (${streamName}):`, 
                (error as Error).message,
            );
        }
    }

    private createWorkingSignal(_msg: JsMsg): () => void {
        const ackWaitMs = this.config.ack_wait / 1_000_000;
        const intervalDelay = Math.floor(ackWaitMs * 0.5);

        const workingInterval = setInterval(() => {
            _msg.working();
        }, intervalDelay);
        
        return () => {
            clearInterval(workingInterval);
        }
    }
}