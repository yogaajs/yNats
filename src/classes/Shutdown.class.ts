// Class
// ===========================================================

export class StaticShutdownManager {
    private static handlers: (() => Promise<void> | void)[] = [];
    private static status: "undefined" | "initialized" | "shutdown" = "undefined";

    // Public

    public static register(handler: () => Promise<void> | void): void {
        this.handlers.push(handler);

        if (this.status === "undefined") {
            this.status = "initialized";
            process.once("SIGINT", () => this.shutdown("SIGINT"));
            process.once("SIGTERM", () => this.shutdown("SIGTERM"));
        }
    }

    // Private

    private static async shutdown(signal: string): Promise<void> {
        if (this.status === "undefined") {
            console.warn("[ShutdownManager] Not initialized.");
            return;
        }
        if (this.status === "shutdown") {
            console.warn("[ShutdownManager] Shutdown already in progress.");
            return;
        }
        this.status = "shutdown";

        console.log(`[ShutdownManager] Received ${signal}. Running cleanup...`);

        // Run all registered cleanup handlers in parallel
        await Promise.all(this.handlers.map(async (handler) => {
            try {
                await handler();
            } catch (err) {
                console.error("[ShutdownManager] Error during cleanup:", err);
            }
        }));

        console.log("[ShutdownManager] Cleanup complete.");
    }
}