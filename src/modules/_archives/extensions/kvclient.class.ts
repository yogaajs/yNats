import { Kvm } from '@nats-io/kv';
import { Client } from 'src/core/Client';
import { Logger } from 'src/classes/logger.class';
import { Readiness } from 'src/classes/manager.class';
import { type IKvStore, type KV, type KvOptions, StorageType, DiscardPolicy } from '../types';

// Class
// ===========================================================

export class KvClient extends Client {
    private readonly _kvName: string;
    private readonly _kvLogger: Logger;
    private readonly _kvReady: Readiness;
    
    private _kvm: Kvm | null = null;
    private _kv: KV | null = null;

    // Constructor

    constructor({ storeName, options }: IKvStore.Options) {
        super();

        this._kvName = storeName;
        this._kvReady = new Readiness();
        this._kvLogger = new Logger(`[nats][kvstore][${storeName}]`);

        this._createOrGetStore(storeName, options);
    }

    // Getters

    protected get kv(): KV {
        if (!this._kv) {
            throw new Error('KV store not initialized');
        }
        return this._kv;
    }

    protected get kvName(): string {
        return this._kvName;
    }

    protected get kvReady(): Readiness {
        return this._kvReady;
    }

    protected get kvLogger(): Logger {
        return this._kvLogger;
    }

    // Private

    private async _createOrGetStore(storeName: string, options?: Partial<KvOptions>): Promise<void> {
        await super.clientReady.isReady();

        // Create the Kv manager
        if (!this._kvm) {
            this._kvm = new Kvm(super.jetstreamClient);
        }

        // Generate options
        const kvOptions = {
            // KvOptions
            timeout: 10_000,                // How long to wait in milliseconds for a response from the KV
            // KvLimits
            replicas: 3,                    // Use 3+ for HA, but 1 is fine locally
            max_bytes: 500 * 1024 * 1024,   // The maximum number of bytes on the KV (500MB)
            ttl: 60 * 60 * 1000,            // The maximum number of millis the key should live in the KV (1 hour)
            storage: StorageType.Memory,    // The storage type for the KV (Memory)
            history: 1,                     // The number of revisions to keep in the Kv (1)
            discard: DiscardPolicy.Old,     // The policy for discarding old revisions (Old)
            // Additional options
            ...(options || {}),
        }

        // Get or create the KV
        if (!this._kv) {
            this._kv = await this._kvm.create(storeName, kvOptions);
        }

        // Ready
        this._kvLogger.info(`KV Store initialized`);
        this._kvReady.setReady();
    }

    // Protected

    protected _encodePayload(data: IKvStore.Payload['data']): string {
        const payload = JSON.stringify({ timestamp: Date.now(), data });
        return payload;
    }

    protected _decodePayload(data: string): IKvStore.Payload['data'] | null {
        try {
            const payload = JSON.parse(data) as IKvStore.Payload;
            return payload.data || null;
        } catch (error) {
            return null;
        }
    }
}
