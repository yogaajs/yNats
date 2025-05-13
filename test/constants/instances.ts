import type { ConnectionOptions } from "@nats-io/transport-node";

//  Constants
// ===========================================================

export const instances = {
   servers: [
      "nats://nats1:4222",
      "nats://nats2:4223",
      "nats://nats3:4224",
   ],
   maxReconnectAttempts: -1,
   reconnect: true,
} satisfies ConnectionOptions;
