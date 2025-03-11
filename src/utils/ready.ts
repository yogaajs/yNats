// Class
// ===========================================================

export class ReadyPromise {
    private status: "pending" | "ready" | "not_ready" = "pending";
    private waitingCount: number = 0;

    private pendingPromise: Promise<void> | null = null;
    private pendingResolve: (() => void) | null = null;
    private pendingReject: (() => void) | null = null;

    // Public

    public setReady(): void {
        if (this.status !== "ready") {
            this.pendingResolve?.();        // Resolve the current promise
            this.status = "ready";          // Update the state to "ready"
        }
    }

    public setNotReady(): void {
        if (this.status !== "not_ready") {
            this.status = "not_ready";      // Update the state to "not ready"
        }
    }

    public isReady(): Promise<void> {
        if (this.status === "ready") {
            return Promise.resolve();   // Immediately resolve if "ready"
        }
        if (!this.pendingPromise) {
            this.pendingPromise = new Promise<void>((resolve, reject) => {
                this.pendingResolve = resolve;
                this.pendingReject = reject;
            });
            this.waitingCount = 0;          // Reset the waiting count
        }
        this.waitingCount++;
        if (this.waitingCount % 10 === 0) {
            console.log(`ReadyManager: ${this.waitingCount} waiting for ready...`);
        }
        return this.pendingPromise;
    }
}
