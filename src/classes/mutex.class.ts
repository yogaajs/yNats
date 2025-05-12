/**
 * Simple mutex implementation that limits concurrent operations
 * Provides lock/unlock functionality with configurable concurrency limit
 */
export default class Mutex {
    /** Maximum number of concurrent operations allowed */
    private readonly maxConcurrent: number;

    /** Temporary upper limit of concurrent operations */
    private concurrentLimit: number = 0;

    /** Temporary upper limit of concurrent operations */
    private concurrentLimitTimeout: NodeJS.Timeout | null = null;
    
    /** Current number of active locks */
    private currentLocks: number = 0;
    
    /** Queue of pending lock requests */
    private waitingQueue: Array<{ resolve: () => void }> = [];

    /**
     * Creates a new mutex instance
     * @param maxConcurrent Maximum number of concurrent operations allowed (default: 1)
     */
    constructor({
        maxConcurrent,
    }: {
        maxConcurrent: number;
    }) {
        if (maxConcurrent <= 0) {
            throw new Error('Maximum concurrent operations must be greater than 0');
        }
        this.maxConcurrent = maxConcurrent;
        this.concurrentLimit = maxConcurrent;
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
        if (this.currentLocks < this.concurrentLimit) {
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

    /**
     * Temporarily sets the upper limit of concurrent operations
     * @param slots The number of slots to increase the limit by
     * @param timeoutMs The timeout in milliseconds
     */
    public setUpperLimitTemporary(slots: number, timeoutMs: number): void {
        if (this.concurrentLimit > this.maxConcurrent * 3) {
            throw new Error('Upper limit is already at the maximum!');
        }
        if (slots < 1) {
            throw new Error('Slots must be greater than 1!');
        }
        if (this.concurrentLimitTimeout) {
            clearTimeout(this.concurrentLimitTimeout);
        }
        this.concurrentLimit = this.concurrentLimit + slots;
        this.concurrentLimitTimeout = setTimeout(() => {
            this.concurrentLimit = this.maxConcurrent;
        }, timeoutMs);
    }
}
