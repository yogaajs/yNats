// Class
// ===========================================================

export class Readiness {
    private _isReady: boolean = false;
    private _pendingPromise: Promise<void> | null = null;
    private _pendingResolve: (() => void) | null = null;
    private _pendingReject: ((reason?: any) => void) | null = null;

    // Public

    public setReady(): void {
        if (this._isReady !== true) {
            this._isReady = true;           // Update the state to "ready"
            if (this._pendingResolve) {
                this._pendingResolve();     // Resolve the current promise
            }
            this._pendingPromise = null;    // Clear the promise
            this._pendingResolve = null;    // Clear the resolve function
            this._pendingReject = null;     // Clear the reject function
        }
    }

    public setNotReady(): void {
        if (this._isReady !== false) {
            this._isReady = false;          // Update the state to "not ready"
            this._pendingPromise = null;    // Clear the promise
            this._pendingResolve = null;    // Clear the resolve function
            this._pendingReject = null;     // Clear the reject function
        }
    }

    public isReady(): Promise<void> {
        if (this._isReady === true) {
            return Promise.resolve();       // Immediately resolve if "ready"
        }
        if (!this._pendingPromise) {
            this._pendingPromise = new Promise<void>((resolve, reject) => {
                this._pendingResolve = resolve;
                this._pendingReject = reject;
            });
        }
        return this._pendingPromise;
    }
}
