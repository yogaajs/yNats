import { KV, KvOptions, KvEntry } from '@nats-io/kv';
import { StorageType, DiscardPolicy } from "@nats-io/jetstream";

// Types
// ===========================================================

export namespace IKvStore {
    export type Options = {
        storeName: string;
        debounceDelayMs: number;
        cacheTtlMs: number;
        options?: Partial<KvOptions>;
    };
    export type Payload = {
        timestamp: number;
        data: Record<string, any>;
    };
};

export { KV, KvOptions, KvEntry };
export { StorageType, DiscardPolicy };