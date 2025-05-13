import { createPromise } from 'src/utils/promise.utils';
import { retryWrapper } from 'src/utils/retry.utils';

// Class
// ===========================================================

interface IQueue {
    promise: Promise<any>;
    resolve: (value: any) => void;
    reject: (reason?: any) => void;
    callback: () => Promise<any>;
}

// Class
// ===========================================================

export class Debouncer {
    private readonly DEBOUNCE_DELAY: number;
    private _queue: Map<string, IQueue> = new Map();
  
    // Constructor

    constructor(debounceDelayMs: number) {
        this.DEBOUNCE_DELAY = debounceDelayMs;
    }
  
    // Public

    public debounce<T>(key: string, callback: () => Promise<T>): Promise<T> {
        if (this._queue.has(key)) {
            const queue = this._queue.get(key)!;    // Get the queue
            queue.callback = callback;              // Update the callback (latest)
            return queue.promise;                   // Return the promise
        }

        // Create a promise that executes callback after timer
        const { promise, resolve, reject } = createPromise<T>();
        this._queue.set(key, { promise, resolve, reject, callback });
        this._createTimeout(key);
        return promise;
    }

    // Private

    private _createTimeout(key: string): void {
        const clearTimer = setTimeout(async () => {
            const queue = this._queue.get(key)!;
            this._queue.delete(key);
            try {
                await retryWrapper({
                    retries: 3,
                    backoff: 1_000,
                    callback: async () => {
                        const result = await queue.callback();
                        queue.resolve(result);
                    }
                });
            } catch (error) {
                queue.reject(error);
            }
        }, this.DEBOUNCE_DELAY);
    }
}