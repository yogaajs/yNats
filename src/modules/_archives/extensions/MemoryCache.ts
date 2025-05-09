/**
 * In-memory cache with size limitations and automatic expiration
 * Provides efficient storage with automatic cleanup mechanisms
 */
export class MemoryCache {
    /** Stores cache data objects with their calculated memory size */
    private dataStore: Map<string, { data: any; size: number }> = new Map();
    
    /** Tracks expiration timestamps for each cached item */
    private expirationsStore: Map<string, number> = new Map();

    /** Current size of all cached items in bytes */
    private currentCacheSizeBytes: number = 0;
    
    /** Maximum allowed size of cache in bytes */
    private maxCacheSizeBytes: number;
    
    /** Maximum age of cache items in milliseconds */
    private maxItemAgeMs: number;
  
    /**
     * Creates a new cache instance
     * @param maxCacheSizeMB Maximum cache size in megabytes (default: 100MB)
     * @param maxItemAgeMinutes Maximum time in minutes before items expire (default: 30 minutes)
     */
    constructor(maxCacheSizeMB: number, maxItemAgeMinutes: number) {
        if (maxCacheSizeMB <= 0) {
            throw new Error('Cache size must be greater than 0 MB');
        }
        if (maxItemAgeMinutes <= 0) {
            throw new Error('Cache item age must be greater than 0 minutes');
        }
        
        this.maxCacheSizeBytes = maxCacheSizeMB * 1024 * 1024;
        this.maxItemAgeMs = maxItemAgeMinutes * 60 * 1000;
        this.startCleanup();
    }
  
    // Public methods

    /**
     * Stores or updates a value in the cache
     * @param key Unique identifier for the cached item
     * @param data The value to store (must be serializable)
     * @param ttlMinutes Optional custom TTL in minutes for this specific item
     * @returns True if the operation was successful
     * @throws Error if the key is invalid or data cannot be serialized
     */
    public set<T>(key: string, data: T, ttlMinutes?: number): boolean {
        if (!key || typeof key !== 'string') {
            throw new Error('Cache key must be a non-empty string');
        }
        
        try {
            // Update size tracking if key already exists
            if (this.dataStore.has(key)) {
                const oldItem = this.dataStore.get(key)!;
                this.currentCacheSizeBytes -= oldItem.size;
            }
        
            // Calculate size and handle potential serialization errors
            let size: number;
            try {
                size = Buffer.byteLength(JSON.stringify(data));
            } catch (error) {
                throw new Error(`Failed to serialize data for key "${key}": ${error instanceof Error ? error.message : String(error)}`);
            }
            
            // Use custom TTL if provided, otherwise use default
            const ttlMs = ttlMinutes !== undefined ? ttlMinutes * 60 * 1000 : this.maxItemAgeMs;
            const expiration = Date.now() + ttlMs;
        
            this.dataStore.set(key, { data, size });
            this.expirationsStore.set(key, expiration);
            this.currentCacheSizeBytes += size;
            
            return true;
        } catch (error) {
            console.error(`[MemoryCache] Error setting item "${key}":`, error);
            return false;
        }
    }
  
    /**
     * Retrieves a value from the cache if it exists and hasn't expired
     * @param key The identifier to look up
     * @param updateExpiration Whether to reset the expiration time when accessed (default: false)
     * @returns The cached value or null if not found or expired
     */
    public get<T>(key: string, updateExpiration: boolean = false): T | null {
        if (!key) return null;
        
        const cachedItem = this.dataStore.get(key);
        if (!cachedItem) return null;
        
        const expiration = this.expirationsStore.get(key)!;
        const now = Date.now();
        
        // Check if item has expired
        if (now > expiration) {
            this.reset(key);
            return null;
        }
        
        // Update expiration if requested (implements sliding expiration)
        if (updateExpiration) {
            this.expirationsStore.set(key, now + this.maxItemAgeMs);
        }
        
        return cachedItem.data as T;
    }
    
    /**
     * Checks if an item exists in the cache and hasn't expired
     * @param key The identifier to check
     * @returns Boolean indicating if the item exists and is valid
     */
    public has(key: string): boolean {
        if (!this.dataStore.has(key)) return false;
        
        const expiration = this.expirationsStore.get(key)!;
        const isExpired = Date.now() > expiration;
        
        if (isExpired) {
            this.reset(key);
            return false;
        }
        
        return true;
    }
  
    /**
     * Removes an item from the cache and updates size tracking
     * @param key The identifier of the item to remove
     * @returns True if the item was found and removed
     */
    public reset(key: string): boolean {
        const cachedItem = this.dataStore.get(key);
        if (!cachedItem) return false;
        
        this.currentCacheSizeBytes -= cachedItem.size;
        this.dataStore.delete(key);
        this.expirationsStore.delete(key);
        
        return true;
    }
    
    // Private methods
  
    /**
     * Initiates periodic cleanup process
     * Removes expired items and enforces cache size limits
     */
    private startCleanup(): void {
        const cleanup = () => {
            const now = Date.now();
            try {
                // Remove expired items
                let expiredCount = 0;
                for (const [key, expiration] of this.expirationsStore.entries()) {
                    if (now > expiration) {
                        this.reset(key);
                        expiredCount++;
                    }
                }

                // Enforce size limits if necessary
                let evictedCount = 0;
                if (this.currentCacheSizeBytes > this.maxCacheSizeBytes) {
                    // Sort by expiration time (oldest first)
                    const sortedItems = Array.from(this.expirationsStore.entries())
                        .sort((a, b) => a[1] - b[1]);
            
                    // Remove oldest items until under target size (75% of max)
                    for (const [key] of sortedItems) {
                        this.reset(key);
                        evictedCount++;
                        
                        if (this.currentCacheSizeBytes <= this.maxCacheSizeBytes * 0.75) {
                            break;
                        }
                    }
                }
                
                // Log cleanup results if anything was removed
                if (expiredCount > 0 || evictedCount > 0) {
                    console.debug(`[MemoryCache] Cleanup: removed ${expiredCount} expired items and evicted ${evictedCount} items due to size constraints.`);
                }
            } catch (err) {
                console.error(`[MemoryCache] Cleanup error:`, err);
            } finally {
                // Schedule next cleanup
                setTimeout(cleanup, 60 * 1000); // run every minute
            }
        };
    
        // Start the cleanup cycle
        cleanup();
    }
}