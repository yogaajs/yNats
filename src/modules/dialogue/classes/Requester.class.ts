import { headers, type MsgHdrs, StorageType, NatsError, ErrorCode, Kv, KvOptions } from 'nats';
import { Client } from '@/client/Client';
import { Logger } from '@/classes/Logger.class';
import { jsonCodec } from '@/utils/nats.utils';
import type { RequestOptions, RequestPayload, ResponsePayload } from '../types';

// Class
// ===========================================================

export class Requester extends Client {
    private readonly _endpoint: string;
    private readonly _options: Partial<KvOptions> | undefined;

    private readonly _logger: Logger = new Logger(`[nats][requester]`);

    private readonly _defaultTimeout = 5_000; // 5 seconds
    private readonly _defaultRetries = 2;
    private readonly _defaultRetryDelay = 250; // 250ms

    private _kv: Kv;

    // Constructor

    constructor({ endpoint, options }: { endpoint: string, options?: Partial<KvOptions> }) {
        super();
        this._endpoint = endpoint;
        this._options = options;
    }

    // Public

    public async get(key: string) {
        if (!this._kv) {
            throw new Error('KV not initialized');
        }

        const value = await this._kv.get(key);
        return jsonCodec.decode(value);
    }

    public async request(key: string, data: Record<string, any>, options?: RequestOptions) {
        const { subject, payload } = this._createPayload(this._endpoint, data);

        const retries = options?.retries ?? this._defaultRetries;
        const retryDelay = options?.retryDelay ?? this._defaultRetryDelay;

        // Wait client ready
        await this.clientReady.isReady();
        await this._createOrGetBucket(this._options);

        // Fetch from key/value store
        const value = await this._kv!.;
        if (value) {
            return jsonCodec.decode(value);
        }

        // Request
        for (let attempt = 1; attempt <= retries; attempt++) {
            try {
                const reply = await this.nc.request(subject, payload, { 
                    timeout: options?.timeout ?? this._defaultTimeout,
                    headers: options?.headers,
                });
        
                // Process the response
                const response = jsonCodec.decode(reply.data) as ResponsePayload<TResponse>;
                
                // If the response contains an error object, throw it
                if (response.error) {
                    throw new Error(`Server error: ${response.error.code} - ${response.error.message}`);
                }
                
                return response.data;
    
            } catch (error) {
                let lastError = error as Error;
                
                // Format error message based on error type
                let message = lastError.message;
                if (error instanceof NatsError) {
                    switch (error.code) {
                        case ErrorCode.NoResponders:
                            message = `No service is listening on ${endpoint}`;
                            break;
                        case ErrorCode.Timeout:
                            message = `Service at ${endpoint} didn't respond`;
                            break;
                        case ErrorCode.ConnectionClosed:
                            message = `Connection closed while waiting for response from ${endpoint}`;
                            break;
                    }
                }
                
                this._logger.warn(`Request to ${endpoint} failed (attempt ${attempt}/${retries}): ${message}`);
                
                // If we have retries left, wait and try again
                if (attempt <= retries) {
                    await new Promise(resolve => setTimeout(resolve, retryDelay));
                    continue;
                }
                
                // No more retries, throw the error
                this._logger.error(`Failed to request from ${endpoint} after ${attempt} attempts:`, message);
                throw lastError;
            }
        }
    }

    // Utility

    public createHeaders(obj: Record<string, string | number | boolean>): MsgHdrs {
        const h = headers();

        for (const [key, value] of Object.entries(obj)) {
            h.append(key, String(value));
        }

        return h;
    }

    // Private

    private async _createOrGetBucket(options?: Partial<KvOptions>): Promise<Kv> {
        if (this._kv) {
            return this._kv;
        }

        // Create a key-value bucket for contracts
        const kv = await this.jsc.views.kv(this._endpoint, {
            timeout: 10_000, // How long to wait in milliseconds for a response from the KV
            //streamName: "contracts", // The underlying stream name for the KV
            // codec: {                    // An encoder/decoder for keys and values
            //     key: {
            //         encode: (key: string) => key, // Keys remain as strings
            //         decode: (key: string) => key, // Keys remain as strings
            //     },
            //     value: {
            //         encode: (value: any) => jsonCodec.encode(value), // Use jsonCodec for values
            //         decode: (data: Uint8Array) => jsonCodec.decode(data),
            //     }
            // }, 
            ttl: 60 * 60 * 1000, // The maximum number of millis the key should live
            storage: StorageType.Memory, // The storage type for the KV
            ...options,
        });

        this._kv = kv.get;
        return kv;
    }

    private _createPayload(endpoint: string, data: Record<string, any>) {
        const subject = `api.${endpoint}`;
        const payload = jsonCodec.encode({ 
            timestamp: Date.now(), 
            data 
        });

        return { subject, payload };
    }
}
