// Class
// ===========================================================

export class Cache<T> {
    private readonly TTL_MS: number;
    private readonly _store = new Map<string, {data: T, expiresAt: number}>();

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
            const { data, expiresAt } = this._store.get(key)!;
            if (expiresAt > Date.now()) {
                return data;
            }
        }
        return undefined;
    }

    public set(key: string, value: T): void {
        this._store.set(key, { data: value, expiresAt: Date.now() + this.TTL_MS });
    }

    public delete(key: string): void {
        this._store.delete(key);
    }

    // Private

    private _cleanup(): void {
        const now = Date.now();
        const array = [...this._store.entries()];
        for (const [key, { expiresAt }] of array) {
            if (expiresAt <= now) {
                this._store.delete(key);
            }
        }
    }
}
