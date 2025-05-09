/**
 * Simple mutex implementation that limits concurrent operations
 * Provides lock/unlock functionality with configurable concurrency limit
 */
export default class Mutex {
    /** Maximum number of concurrent operations allowed */
    private readonly maxConcurrent: number;
    
    /** Current number of active locks */
    private currentLocks: number = 0;
    
    /** Queue of pending lock requests */
    private waitingQueue: Array<{ resolve: () => void }> = [];

    /**
     * Creates a new mutex instance
     * @param maxConcurrent Maximum number of concurrent operations allowed (default: 1)
     */
    constructor(maxConcurrent: number = 1) {
        if (maxConcurrent <= 0) {
            throw new Error('Maximum concurrent operations must be greater than 0');
        }
        this.maxConcurrent = maxConcurrent;
    }

    /**
     * Returns the current number of active locks
     */
    public get activeCount(): number {
        return this.currentLocks;
    }

    /**
     * Returns the number of pending lock requests
     */
    public get waitingCount(): number {
        return this.waitingQueue.length;
    }

    /**
     * Acquires a lock, waiting if necessary until one becomes available
     * @returns A promise that resolves when the lock is acquired
     */
    public lock(): Promise<void> {
        // If we haven't reached the maximum concurrent operations, grant the lock immediately
        if (this.currentLocks < this.maxConcurrent) {
            this.currentLocks++;
            return Promise.resolve();
        }

        // Otherwise, add to the waiting queue
        return new Promise<void>((resolve, reject) => {
            this.waitingQueue.push({ resolve });
        });
    }

    /**
     * Releases a previously acquired lock
     * If there are waiting lock requests, the next one will be granted
     */
    public unlock(): void {
        // Ensure we don't decrement below zero
        if (this.currentLocks <= 0) {
            return;
        }

        // Decrement the current lock count
        this.currentLocks--;

        // If there are waiting requests and we have capacity, grant the next lock
        if (this.waitingQueue.length > 0) {
            const next = this.waitingQueue.shift();
            if (next) {
                this.currentLocks++;
                next.resolve();
            }
        }
    }
}
