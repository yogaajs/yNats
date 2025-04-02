// Types
// ===========================================================

interface RetryOptions<T> {
    retries: number;                // Number of retry attempts
    backoff: number;                // Initial backoff delay in ms
    maxBackoff?: number;            // Maximum backoff delay (default: 10000ms)
    callback: () => Promise<T>;     // Callback on retry
}

/**
 * Executes an asynchronous function with auto-retry capability.
 * 
 * @param options - Retry configuration options
 * @returns Promise with the callback result
 * @throws Last encountered error after all retries are exhausted
 */
export async function retryWrapper<T>(options: RetryOptions<T>): Promise<T> {
    const { retries, backoff, maxBackoff, callback } = options;

    for (let attempts = 1; attempts <= retries; attempts++) {
        try {
            return await callback();
        } catch (err) {
            if (attempts >= retries) {
                // All retries exhausted, rethrow the error.
                throw err;
            }
            const delay = Math.min(backoff * Math.pow(2, attempts), maxBackoff ?? 10_000);
            await new Promise((resolve) => setTimeout(resolve, delay));
        }
    }
    throw new Error('Max retries exceeded');
}