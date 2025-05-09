// Class
// ===========================================================

export default class Manager {
    private ready: boolean = false;
    private promise: Promise<void> | null = null;
    private resolve: (() => void) | null = null;
    private reject: ((reason?: any) => void) | null = null;

    // Public

    public setReady(resolve: boolean = true): void {
        if (this.ready !== true) {
            this.ready = true;      // Update the state to "ready"
            if (resolve) {
                this.resolve?.();   // Resolve the current promise
            }
            this.clear();
        }
    }

    public setNotReady(reject: boolean = false): void {
        if (this.ready !== false) {
            this.ready = false;     // Update the state to "not ready"
            if (reject) {
                this.reject?.();    // Reject the current promise
            }
            this.clear();
        }
    }

    public isReady(): Promise<void> {
        if (this.ready === true) {
            return Promise.resolve();       // Immediately resolve if "ready"
        }
        if (!this.promise) {
            this.promise = new Promise<void>((resolve, reject) => {
                this.resolve = resolve;
                this.reject = reject;
            });
        }
        return this.promise;
    }

    // Private

    private clear() {
        this.promise = null;    // Clear the promise
        this.resolve = null;    // Clear the resolve function
        this.reject = null;     // Clear the reject function
    }
}
