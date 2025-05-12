import type { Subscription, MsgHdrs } from '@nats-io/nats-core';
import type { PubAck } from '@nats-io/jetstream';
import type { Client } from "src/core/client.class";
import { withExpiration } from '@/utils/timeout.utils';
import { setSubject, setHeader, getHeader, setInbox } from '../utils';
import { streamConfig } from '../config';
import ApiBase, { type API } from '../classes/common.class';

// Types
// ===========================================================

export namespace ApiRequester {
    export type Config = {
        streamName: string,
        streamMaxConsumers?: number,
        streamMaxAgeSeconds?: number,
        streamMaxMegabytes?: number,
    };
    export type Options = {
        maxAttempts: number;
        timeoutProcess: number;
        warnThreshold: number;
        maxConcurrent: number,
        debug: boolean;
    };
};

// Class
// ===========================================================

export class ApiRequester extends ApiBase {
    private readonly config: API.StreamConfig;
    private readonly options: Omit<ApiRequester.Options, 'maxConcurrent' | 'debug'>;

    private queue: Array<{ subject: string, payload: string, resolve: (value: any) => void, reject: (reason?: any) => void }> = [];
    private isProcessing: boolean = false;
    private isShuttingDown: boolean = false;

    // Constructor

    constructor(
        client: Client,
        config: ApiRequester.Config,
        options?: Partial<ApiRequester.Options>
    ) {
        super(client, {
            type: 'requester',
            streamName: config.streamName,
            maxConcurrent: options?.maxConcurrent,
            debug: options?.debug,
        });

        // Options (class)
        this.options = {
            maxAttempts: options?.maxAttempts ?? 2,
            timeoutProcess: options?.timeoutProcess ?? 15_000,
            warnThreshold: options?.warnThreshold ?? 100,
        } satisfies Omit<ApiRequester.Options, 'maxConcurrent' | 'debug'>;

        // Config (stream)
        this.config = streamConfig({
            streamName: config.streamName,
            streamMaxConsumers: config?.streamMaxConsumers,
            streamMaxAgeSeconds: config?.streamMaxAgeSeconds,
            streamMaxMegabytes: config?.streamMaxMegabytes,
        }) satisfies ReturnType<typeof streamConfig>;

        // Setup
        this.setupStream(this.config)
            .then(() => this.manager.setReady());
    }

    // Public

    public async request<T>(_subject: string, _data: Record<string, any>): Promise<T> {
        // Constants
        const subject = setSubject(this.config.name, _subject);
        const payload = JSON.stringify({ timestamp: Date.now(), data: _data });

        // Execute the request
        const result = (new Promise((resolve, reject) => {
            this.queue.push({ subject, payload, resolve, reject });
            this.processRequests();
        }));

        // Warn if the number of requests is too high
        if (this.queue.length > this.options.warnThreshold) {
            this.logger.alert('queue', `High number of requests: ${this.queue.length}`);
            try {
                this.mutex.setUpperLimitTemporary(5, 10_000);
            } catch (error) {
                this.logger.error((error as Error).message);
            }
        }
        
        // Wait for the result
        return (await result) as T;
    }

    public async shutdown(): Promise<void> {
        if (this.isShuttingDown) {
            return;
        }

        this.isShuttingDown = true;
        this.logger.info(`Shutting down requester...`);

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
            this.isShuttingDown = false;
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

                // Lock the mutex
                await this.mutex.lock();

                // Process the request (unlock the mutex)
                this.processRequest()
                    .finally(() => this.mutex.unlock());
            }

        } finally {
            this.isProcessing = false;
            setImmediate(() => this.processRequests());
        }
    }

    private async processRequest(): Promise<void> {
        // Process the queue
        const { subject, payload, resolve, reject } = this.queue.shift()!;

        // Create headers and reply data
        const inbox = setInbox(subject);
        const headers = setHeader(inbox);

        // Attempt to publish the message
        for (let attempt = 1; attempt <= this.options.maxAttempts; attempt++) {
            const expireAt = Date.now() + this.options.timeoutProcess;
            let subscription: Subscription | null = null;
            try {
                // Ensure the class is ready
                await withExpiration(this.isReady(), 'Timeout (class ready)', expireAt);

                // Create the subscription
                subscription = this.client.natsConnection.subscribe(inbox);
                const responsePromise = this.waitResponse(subscription, inbox);

                // Send the request
                const sendPromise = this.sendMessage(subject, payload, headers);
                await withExpiration(sendPromise, 'Timeout (send request)', expireAt);

                // Wait for the response
                const result = await withExpiration(responsePromise, 'Timeout (wait response)', expireAt);
                resolve(result);
                break;

            } catch (error) {
                this.logger.error(`Failed to process request in "${subject}" (attempt ${attempt}):`,
                    (error as Error).message,
                );
            } finally {
                subscription?.unsubscribe?.();
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

    private waitResponse(_subscription: Subscription, _inbox: string): Promise<unknown> {
        return (async () => {
            for await (const msg of _subscription) {
                if (getHeader(msg.headers!) === _inbox) {
                    return msg.json();
                }
            }
            throw new Error('Subscription closed before response');
        })();
    }

    private sendMessage(_subject: string, _payload: string, _headers: MsgHdrs): Promise<PubAck> {
        return this.client.jetstreamClient.publish(_subject, _payload, {
            headers: _headers,
            timeout: 10_000,
        });
    }
}
