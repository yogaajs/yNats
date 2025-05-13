import type { PubAck, StreamConfig } from '@nats-io/jetstream';
import type { Client } from "src/core/client.class";
import { Common } from '../classes/common.class';
import { withExpiration } from 'src/utils/timeout.utils';
import { setSubject } from '../utils';

// Types
// ===========================================================

export namespace StreamPublisher {
    export type Config = {
        streamName: StreamConfig['name'],
        streamRetention: StreamConfig['retention'],
        streamStorage: StreamConfig['storage'],
        streamMaxConsumers?: number,
        streamMaxAgeSeconds?: number,
        streamMaxMegabytes?: number,
    };
    export type Options = {
        maxAttempts: number;
        timeoutProcess: number;
        warnThreshold: number;
        debug: boolean;
    }; 
};

// Class
// ===========================================================

export class StreamPublisher extends Common<'publisher'> {
    private readonly options: StreamPublisher.Options;

    private queue: Array<{ subject: string, payload: string, resolve: (value: PubAck) => void, reject: (reason?: any) => void }> = [];
    private isProcessing: boolean = false;
    private isShutdown: boolean = false;

    // Constructor

    constructor(
        client: Client,
        config: StreamPublisher.Config,
        options?: Partial<StreamPublisher.Options>
    ) {
        super(client, config);

        // Options
        this.options = {
            maxAttempts: options?.maxAttempts ?? 2,
            timeoutProcess: options?.timeoutProcess ?? 60_000,
            warnThreshold: options?.warnThreshold ?? 100,
            debug: options?.debug ?? false,
        } satisfies StreamPublisher.Options;

        // Setup
        this.setupStream()
            .then(() => {
                this.manager.setReady();
            });
    }

    // Public

    public async publish(_subject: string, _data: Record<string, any>): Promise<PubAck> {
        // Constants
        const subject = setSubject(this.streamConfig.name, _subject);
        const payload = JSON.stringify({ timestamp: Date.now(), data: _data });

        // Execute the request
        const result = (new Promise((resolve, reject) => {
            this.queue.push({ subject, payload, resolve, reject });
            this.processRequests();
        }));

        // Warn if the number of requests is too high
        if (this.queue.length > this.options.warnThreshold) {
            this.logger.alert('queue', 10_000, `High number of requests active: ${this.queue.length}`);
        }
        
        // Wait for the result
        return (await result) as PubAck;
    }

    public async shutdown(): Promise<void> {
        if (this.isShutdown) {
            return;
        }

        this.isShutdown = true;
        this.logger.info(`Shutting down publisher...`);

        try {
            while (true) {
                // Wait for all requests to finish
                while (this.queue.length > 0) {
                    this.logger.info(`Waiting ${this.queue.length} requests to process...`);
                    await new Promise(resolve => setTimeout(resolve, 1_000));
                }

                // Grace period
                await new Promise(resolve => setTimeout(resolve, 10_000));

                if (this.queue.length === 0) {
                    break; // All requests finished
                }
            }
        } finally {
            this.isShutdown = false;
            this.logger.info('All requests finished!');
        }
    }

    // Private

    private async processRequests(): Promise<void> {
        if (this.isProcessing || this.queue.length === 0) {
            return;
        }

        // Set the processing flag
        this.isProcessing = true;

        try {
            // Process the queue
            while (this.queue.length > 0) {
                const { subject, payload, resolve, reject } = this.queue.shift()!;

                // Attempt to publish the message
                for (let attempt = 1; attempt <= this.options.maxAttempts; attempt++) {
                    const expireAt = Date.now() + this.options.timeoutProcess;
                    try {
                        // Ensure the class is ready
                        await withExpiration(this.isReady(), 'Timeout (class ready)', expireAt);

                        // Send the request
                        const sendPromise = this.sendMessage(subject, payload);
                        const result = await withExpiration(sendPromise, 'Timeout (send request)', expireAt);
                        resolve(result);
                        return;

                    } catch (error) {
                        this.logger.error(`Failed to process request in "${subject}" (attempt ${attempt}):`,
                            (error as Error).message,
                        );
                    }

                    // Wait for the next attempt
                    if (attempt < this.options.maxAttempts) {
                        await new Promise(resolve => setTimeout(resolve, 100));
                    } else {
                        reject(new Error(`Failed to process request after ${this.options.maxAttempts} attempts!`, {
                            cause: { subject, payload },
                        }));
                    }
                }
            }
        } finally {
            this.isProcessing = false;
            setImmediate(() => this.processRequests());
        }
    }

    private sendMessage(_subject: string, _payload: string): Promise<PubAck> {
        return this.client.jetstreamClient.publish(_subject, _payload, {
            timeout: 15_000,
        });
    }
}
