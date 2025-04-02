// Class
// ===========================================================

export class Cache<T> {
    private readonly TTL_MS: number;
    private readonly _store = new Map<string, T>();
    private readonly _expirations = new Map<string, number>();

    // Constructor

    constructor(ttlMs: number) {
        this.TTL_MS = ttlMs;

        // Start a global cleanup timer.
        setInterval(() => this._cleanup(), ttlMs * 3);
    }

    // Public

    public has(key: string): boolean {
        return this._store.has(key);
    }

    public get(key: string): T | undefined {
        if (this._store.has(key)) {
            this._expirations.set(key, Date.now());
            return this._store.get(key)!;
        }
        return undefined;
    }

    public set(key: string, value: T): void {
        this._store.set(key, value);
        this._expirations.set(key, Date.now());
    }

    public delete(key: string): void {
        this._store.delete(key);
        this._expirations.delete(key);
    }

    // Private

    private _cleanup(): void {
        const now = Date.now();
        const array = [...this._expirations.entries()];
        for (const [key, addedAt] of array) {
            if (addedAt + this.TTL_MS <= now) {
                this._store.delete(key);
                this._expirations.delete(key);
            }
        }
    }
}
