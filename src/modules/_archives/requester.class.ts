import type { IProducer } from '../types';
import { headers, createInbox, Subscription, MsgHdrs } from '@nats-io/nats-core';

import Logger from 'src/classes/logger.class';
import { PubAck } from '@nats-io/jetstream';
import { withExpiration } from 'src/utils/timeout.utils';

// Types
// ===========================================================

export type MessageSubjects = string[];
export type MessageData = Record<string, any>;

// Class
// ===========================================================

export class StreamRequester {
    protected readonly logger: Logger;
    protected readonly client: IProducer['client'];
    protected readonly stream: IProducer['stream'];
    protected readonly options: IProducer['options'];

    protected requestActive: number = 0;

    // Constructor

    constructor({ client, stream, options }: IProducer) {
        if (!client) {
            throw new Error("Client is required!");
        }
        if (!stream) {
            throw new Error("Stream is required!");
        }

        // Private
        this.client = client;
        this.stream = stream;
        this.logger = new Logger(`[nats][requester][${stream.config.name}]`);

        // Public
        this.options = {
            maxAttempts: 2,
            ...options,
        };

        // Register cleanup
        this.client.clientCleaner.register(() => this.finish());
    }

    // Public

    public async publish(_subjects: MessageSubjects, _data: MessageData, _timeout: number = 60_000): Promise<PubAck> {
        this.requestActive++;
        try {
            // Constants
            const expireAt = Date.now() + Math.max(_timeout, 10_000);
            const streamName = this.stream.config.name;

            // Process the message
            const subject = `${streamName}.${_subjects.join('.')}`;
            const payload = JSON.stringify({ timestamp: Date.now(), data: _data });

            return await this.processPublish(subject, payload, expireAt);

        } finally {
            this.requestActive--;
        }
    }

    public async request<T>(_subjects: MessageSubjects, _data: MessageData, _timeout: number = 60_000): Promise<T> {
        this.requestActive++;
        try {
            // Constants
            const expireAt = Date.now() + Math.max(_timeout, 10_000);
            const streamName = this.stream.config.name;

            // Process the request
            const subject = `${streamName}.${_subjects.join('.')}`;
            const payload = JSON.stringify({ timestamp: Date.now(), data: _data });

            return await this.processRequest<T>(subject, payload, expireAt);

        } finally {
            this.requestActive--;
        }
    }

    public async finish(): Promise<void> {
        while (true) {
            // Wait for all requests to finish
            while (this.requestActive > 0) {
                this.logger.info(`Waiting all requests (${this.requestActive}) to finish...`);
                await new Promise(resolve => setTimeout(resolve, 1_000));
            }

            // Grace period
            await new Promise(resolve => setTimeout(resolve, 10_000));

            if (this.requestActive === 0) {
                // All requests finished
                break;
            }
        }

        // Log the shutdown
        this.logger.info('All requests finished!');
    }

    // Private

    private async processPublish(_subject: string, _payload: string, _expireAt: number): Promise<PubAck> {
        // Options
        const maxAttempts = this.options.maxAttempts ?? 2;

        // Attempt to publish the message
        for (let attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                // Ensure the client and stream are ready
                await withExpiration(this.client.isReady(), 'Timeout (client ready)', _expireAt);
                await withExpiration(this.stream.isReady(), 'Timeout (stream ready)', _expireAt);

                // Send the message
                return await withExpiration(
                    this.sendMessage(_subject, _payload), 
                    'Timeout (publish message)',
                    _expireAt
                );

            } catch (error) {
                this.logger.error(`Failed to publish message (attempt ${attempt}):`,
                    (error as Error).message,
                );
            }

            // Wait for the next attempt
            const delay = Math.min(1_000 * attempt, 5_000); // 5s max delay
            await new Promise(resolve => setTimeout(resolve, delay));
        }

        // Throw an error if the request failed
        throw new Error(`Failed to publish message after ${maxAttempts} attempts:`, {
            cause: {
                subject: _subject,
                payload: _payload,
            },
        });
    }

    private async processRequest<T>(_subject: string, _payload: string, _expireAt: number): Promise<T> {
        // Options
        const maxAttempts = this.options.maxAttempts ?? 2;

        // Create headers and reply data
        const uuid = crypto.randomUUID();
        const inbox = createInbox(_subject);
        const headers = this.setHeaders(uuid, inbox);

        // Attempt to publish the message
        for (let attempt = 1; attempt <= maxAttempts; attempt++) {
            let subscription: Subscription | null = null;
            try {
                // Ensure the client and stream are ready
                await withExpiration(this.client.isReady(), 'Timeout (client ready)', _expireAt);
                await withExpiration(this.stream.isReady(), 'Timeout (stream ready)', _expireAt);

                // Create the inbox & subscribe to it
                subscription = this.client.natsConnection.subscribe(inbox);

                // Send the request
                await withExpiration(
                    this.sendMessage(_subject, _payload, headers), 
                    'Timeout (send request)',
                    _expireAt
                );

                // Wait for the response
                return await withExpiration(
                    this.waitResponse<T>(subscription, inbox, uuid), 
                    'Timeout (wait response)', 
                    _expireAt
                );

            } catch (error) {
                this.logger.error(`Failed to process request (attempt ${attempt}):`,
                    (error as Error).message,
                );
            } finally {
                subscription?.unsubscribe?.();
            }

            // Wait for the next attempt
            const delay = Math.min(1_000 * attempt, 5_000); // 5s max delay
            await new Promise(resolve => setTimeout(resolve, delay));
        }

        // Throw an error if the request failed
        throw new Error(`Failed to process request after ${maxAttempts} attempts!`, {
            cause: {
                subject: _subject,
                payload: _payload,
            },
        });
    }

    private setHeaders(_uuid: string, _inbox: string): MsgHdrs {
        const hdr = headers();

        hdr.set('reply-uuid', _uuid);
        hdr.set('reply-inbox', _inbox);

        return hdr;
    }

    private sendMessage(_subject: string, _payload: string, _headers?: MsgHdrs): Promise<PubAck> {
        const streamName = this.stream.config.name;

        return this.client.jetstreamClient.publish(_subject, _payload, {
            headers: _headers,
            timeout: 15_000,
            expect: { 
                streamName,
            },
        });
    }

    private waitResponse<T>(_subscription: Subscription, _inbox: string, _uuid: string): Promise<T> {
        return (async () => {
            for await (const msg of _subscription) {
                if (
                    msg.headers?.get('reply-inbox') === _inbox &&
                    msg.headers?.get('reply-uuid') === _uuid
                ) {
                    return msg.json() as T;
                }
            }
            throw new Error('Subscription closed before response');
        })();
    }
}
