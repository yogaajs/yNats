import { createPromise } from '@/utils/promise.utils';

// Types
// ===========================================================

export interface IOperator {
    add(className: string): () => void;
    waitForAll(logCallback: (name: string, count: number) => void, timeoutMs?: number): Promise<void>;
}

// Class
// ===========================================================

export class Operator {
    private countOperations = new Map<string, number>();
    private activeOperations = new Set<Promise<any>>();

    // Public

    public add(className: string): () => void {
        const { promise, resolve } = createPromise<void>();

        // Add the operation to the count of operations for this class.
        const operationCount = this.countOperations.get(className) ?? 0;
        this.countOperations.set(className, operationCount + 1);

        // Track the operation
        this.activeOperations.add(promise);

        // Remove the operation from the count of operations for this class.
        promise.finally(() => {
            const count = this.countOperations.get(className) ?? 0;
            if (count > 1) {
                this.countOperations.set(className, count - 1);
            } else {
                this.countOperations.delete(className);
            }
            this.activeOperations.delete(promise);
        });

        return () => resolve();
    }

    public async waitForAll(logCallback: (name: string, count: number) => void, timeoutMs: number = 10_000): Promise<void> {
        if (this.activeOperations.size === 0) { return; }

        let watchdogTimer: NodeJS.Timeout | null = null;

        while (this.activeOperations.size > 0) {
            // Reset watchdog timer
            if (watchdogTimer) {
                clearTimeout(watchdogTimer);
            }

            // Set the watchdog timer
            watchdogTimer = setTimeout(() => {
                throw new Error(`Timeout waiting for operations after ${timeoutMs}ms`);
            }, timeoutMs);

            try {
                // Wait until one of the active operations settles
                await Promise.race([...this.activeOperations.values()]);

                // Log counts per operation type
                for (const [className, count] of this.countOperations.entries()) {
                    if (count > 0) {
                        logCallback(className, count);
                    }
                }
            } catch (error) {
                // If a promise rejects, continue with next iteration
                // The watchdog will be reset on next loop
                continue;
            }
        }

        // Always clear the watchdog when done
        if (watchdogTimer) clearTimeout(watchdogTimer);
    }
}
