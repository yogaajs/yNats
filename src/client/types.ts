import type { ConnectionOptions } from "nats";

// Types
// ===========================================================

export interface ClientConstructor {
    options: ConnectionOptions;
}
