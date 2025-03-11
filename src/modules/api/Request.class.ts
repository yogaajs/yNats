import type {  JsMsg, ConsumerMessages, Consumer } from "nats";
import type { ConsumerConstructor, MessagePayload } from './types';
import { Client } from '@/client/Client';
import { ReadyPromise } from 'src/utils/ready';
import { ShutdownManager } from '@/classes/Shutdown.class';
import { sc } from './utils';
import { consumerOpts, AckPolicy, DeliverPolicy } from 'nats';

// Class
// ===========================================================

export class StreamConsumer extends Client {
    private readonly streamName: string;
    private readonly consumerName: string;
    private readonly consumerConfig: ConsumerConstructor['consumerConfig'];

    private cm: ConsumerMessages | null = null;

    // Constructor

    constructor({ streamName, consumerName, consumerConfig }: ConsumerConstructor) {
        super();

        this.streamName = streamName;
        this.consumerName = consumerName;
        this.consumerConfig = consumerConfig;
    }

    // Public

    public async getMessage(msgId: string | number) {
        await this.isClientReady();
        try {
            // Build the subject that corresponds to this id.
            const subject = `${this.streamName}.${msgId}`;

            // Retrieve the message at the given sequence
            const msg = await this.jsm!.streams.getMessage(this.streamName, { last_by_subj: subject });

            // Decode the message
            const stringifiedMessage = sc.decode(msg.data);
            const message = JSON.parse(stringifiedMessage) as MessagePayload;
            return message.data;

        } catch (error: any) {
            this.error(`Error retrieving message ${msgId}:`, (error as Error).message);
            throw error;
        }
    }

    public async getLastMessage() {
        await this.isClientReady();
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
            this.error("Error retrieving last message:", (error as Error).message);
            throw error;
        }
    }

    public async subscribe(handler: (data: any) => Promise<void>): Promise<void> {
        // Ensure client is ready
        await this.isClientReady();

        // Subscribe to the subject for API requests
        const sub = this.nc!.subscribe("api.request", { queue: "workers" });

        (async () => {
            for await (const msg of sub) {
            try {
                const requestData = JSON.parse(msg.data.toString());
                console.log("Received request:", requestData);

                // Process the request (business logic here)
                const result = { success: true, message: `Processed ${JSON.stringify(requestData)}` };

                // Send a reply with the result
                msg.respond(Buffer.from(JSON.stringify(result)));
            } catch (error) {
                console.error("Error processing request:", error);
                msg.respond(Buffer.from(JSON.stringify({ success: false, error: (error as Error).message })));
            }
            }
        })();
    }

    public async unsubscribe(): Promise<void> {
        if (this.cm) {
            this.cm.stop();
            this.info(`Consumer unsubscribed.`);
        }
    }

    // Private

    private async createOrUpdateConsumer(): Promise<Consumer> {
        try {
            // Check if consumer exists
            const info = await this.jsm!.consumers.info(this.streamName, this.consumerName);

            // Merge existing config with new config
            const updatedConfig = { ...info.config, ...this.consumerConfig };

            // Update the consumer
            await this.jsm!.consumers.update(this.streamName, this.consumerName, updatedConfig);

            // Log the update
            this.info(`Consumer "${info.name}" updated.`);

        } catch (err: any) {

            // Check if error indicates that the consumer was not found.
            if (err.message && err.message.includes("consumer not found")) {

                // Create the consumer on the stream.
                await this.jsm!.consumers.add(this.streamName, {
                    durable_name: this.consumerName,
                    ack_policy: AckPolicy.Explicit,
                    deliver_policy: DeliverPolicy.All,
                    ...this.consumerConfig,
                });

                // Log the creation
                this.info(`Consumer "${this.consumerName}" created.`);

            } else {
                // For any other error, rethrow it.
                throw err;
            }
        }

        const consumer = await this.jsc!.consumers.get(this.streamName, this.consumerName);
        return consumer;
    }

    // Private

    private info(...args: any[]): void {
        console.info(`[nats][${this.streamName}][${this.consumerName}]`, ...args);
    }

    private warn(...args: any[]): void {
        console.warn(`[nats][${this.streamName}][${this.consumerName}]`, ...args);
    }

    private error(...args: any[]): void {
        console.error(`[nats][${this.streamName}][${this.consumerName}]`, ...args);
    }
}
