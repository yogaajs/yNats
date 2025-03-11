import type { ConnectionOptions } from "nats";

//  Constants
// ===========================================================

export const options = {
   servers: [
      "nats://nats:4222",
   ],
   maxReconnectAttempts: -1,
   reconnect: true,
} satisfies ConnectionOptions;
