import { KvCommon } from './Common.class';
import { Cache } from '../extensions/Cache.class';
import { Debouncer } from '../extensions/Debounce.class';
import { type IKvStore } from '../types';

// Class
// ===========================================================

export class KvStore extends KvCommon {
    private readonly _kvCache: Cache<Record<string, any>>;
    private readonly _kvDebounce: Debouncer;

    // Constructor

    constructor(options: IKvStore.Options) {
        super(options);
        
        this._kvCache = new Cache(options.cacheTtlMs);
        this._kvDebounce = new Debouncer(options.debounceDelayMs);
    }

    // Public

    public async getAllKeys(filter?: string | string[]): Promise<string[]> {
        await this.clientReady.isReady();
        await this.kvReady.isReady();

        // Fetch all keys (iterator)
        const keysIterator = await this.kv.keys(filter);

        // Convert iterator to array
        const keys: string[] = [];
        for await (const key of keysIterator) {
            keys.push(key);
        }

        return keys;
    }

    public async get(key: string): Promise<Record<string, any> | null> {
        const cached = this._kvCache.get(key);
        if (cached) {
            return cached;
        }

        await this.clientReady.isReady();
        await this.kvReady.isReady();

        // Get from KV store (nats)
        const entry = await this.kv.get(key);
        if (entry) {
            const payload = this._decodePayload(entry.string());
            if (payload) {
                this._kvCache.set(key, payload.data);
                return payload.data;
            }
        }

        // Nothing found
        return null;
    }

    public async put(key: string, data: Record<string, any>): Promise<void> {
        this._kvCache.set(key, data);                   // Update cache
        const payload = this._encodePayload(data);      // Encode payload

        const clearOperation = super.clientOperator.add(this.kvName);

        try {
            await this.clientReady.isReady();
            await this.kvReady.isReady();

            await this._kvDebounce.debounce(key, async () => {
                return await this.kv.put(key, payload);
            });

        } catch (error) {
            this.kvLogger.error(`Failed to put key ${key}`, error);
            throw error;

        } finally {
            clearOperation();
        }
    }

    public async purge(key: string): Promise<void> {
        this._kvCache.delete(key);                      // Update cache

        await this.clientReady.isReady();
        await this.kvReady.isReady();
        
        const clearOperation = super.clientOperator.add(this.kvName);

        try {
            await this._kvDebounce.debounce(key, async () => {
                return await this.kv.purge(key);
            });

        } catch (error) {
            this.kvLogger.error(`Failed to purge key ${key}`, error);
            throw error;

        } finally {
            clearOperation();
        }
    }

    public async watch(key: string, callback: (revision: number, value: string) => void): Promise<() => void> {
        await super.clientReady.isReady();
        await super.kvReady.isReady();

        // KV Iterator
        const watch = await this.kv.watch();

        // Watch
        (async () => {
            for await (const e of watch) {
                try {   
                    callback(e.revision, e.string());
                } catch (error) {
                    this.kvLogger.error(`Failed to watch key ${key}`, e, error);
                }
            }
        })();

        return () => watch.stop();
    }
}
