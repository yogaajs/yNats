// Types
// ===========================================================

interface PromiseOptions<T> { 
    promise: Promise<T>, 
    resolve: (value: T) => void, 
    reject: (reason?: any) => void 
}

/**
 * Creates a promise with a resolve and reject function.
 * 
 * @returns Promise with the callback result
 */
export function createPromise<T>(): PromiseOptions<T> {
    let resolve!: (value: T) => void;
    let reject!: (reason?: any) => void;
    const promise = new Promise<T>((res, rej) => {
        resolve = res;
        reject = rej;
    });
    return { promise, resolve, reject };
}
