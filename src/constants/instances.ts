import type { ConnectionOptions } from "@nats-io/transport-node";

//  Constants
// ===========================================================

export const instances = {
   servers: [
      "nats://nats-1:4222",
      "nats://nats-2:4223",
      "nats://nats-3:4224",
   ],
   maxReconnectAttempts: -1,
   reconnect: true,
} satisfies ConnectionOptions;
