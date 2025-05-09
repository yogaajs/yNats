  // Types
// ===========================================================

type TrackedPromise<T = void> = {
    promise: Promise<T>;
    isSettled: boolean;
};
  
// Class
// ===========================================================

export default class Cleaner {
    private cleanupFunctions = new Set<() => Promise<void>>();

    // Public

    public register(cleanup: () => Promise<void>): void {
        const wrappedCleanup = async () => {
            try {
                await cleanup();
            } catch (err) {
                console.error('[Cleaner] Cleanup function failed:', err);
            } finally {
                this.cleanupFunctions.delete(wrappedCleanup);
            }
        };

        this.cleanupFunctions.add(wrappedCleanup);
    }

    public async cleanAll(callback: (remaining: number) => void, timeoutMs: number = 30_000): Promise<void> {
        // Create the tracked promises
        const pending = this.createTrackedPromises();
        let watchdog: NodeJS.Timeout | null = null;

        while (this.cleanupFunctions.size > 0) {
            if (watchdog) {
                clearTimeout(watchdog);
            }

            // Setup watchdog
            watchdog = setTimeout(() => {
                throw new Error(`[Cleaner] Timeout after ${timeoutMs}ms`);
            }, timeoutMs);

            try {
                // Wait for at least one to settle or timeout
                await Promise.race(pending);
            } catch (err) {
                console.error('[Cleaner] Error during cleanup:', err);
            }

            // Filter out finished ones
            callback(this.cleanupFunctions.size);
        }

        if (watchdog) {
            clearTimeout(watchdog);
        }
    }

    // Private

    private createTrackedPromises(): Set<Promise<void>> {
        const pending: Set<Promise<void>> = new Set();

        for (const cleanup of this.cleanupFunctions) {
            const promise = cleanup();
            promise.finally(() => {
                pending.delete(promise);
            });
            pending.add(promise);
        }

        return pending;
    }
}
