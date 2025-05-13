import type { StreamConfig, ConsumerConfig } from '@nats-io/jetstream';
import type { ApiRequester } from './modules/requester.class';
import type { ApiResponder } from './modules/responder.class';
import { AckPolicy, DeliverPolicy, RetentionPolicy, DiscardPolicy, StorageType, ReplayPolicy } from '@nats-io/jetstream';

// Stream
// ===========================================================

export const streamConfig = ({
    streamName,
    streamMaxConsumers,
    streamMaxAgeSeconds,
    streamMaxMegabytes,
}: ApiRequester.Config) => {
    const maxConsumers = (streamMaxConsumers ?? 10);
    const maxAgeSeconds = (streamMaxAgeSeconds ?? 1 * 60 * 60);
    const maxMegabytes = (streamMaxMegabytes ?? 512);
    return {
        "name": streamName,                           // Set the name
        "subjects": [`${streamName}.>`],              // Set the subjects
        "retention": RetentionPolicy.Workqueue,       // Set the retention
        "storage": StorageType.Memory,                // Set the storage
        "discard": DiscardPolicy.Old,                 // Set the discard policy
        "max_msgs": 10_000,                           // Max messages in the stream
        "max_msgs_per_subject": 1_000,                // Per subject retention limit
        "max_msg_size": -1,                           // Max message size
        "max_consumers": maxConsumers,                // Max consumers for this stream
        "max_age": maxAgeSeconds * 1_000_000_000,     // Nanoseconds
        "max_bytes": maxMegabytes * 1024 * 1024,      // MB
    } satisfies Partial<StreamConfig>;
};

// Consumers
// ===========================================================

export const consumerConfig = ({
    streamName,
    consumerName,
    filterSubject,
}: ApiResponder.Config) => {
    const durableName = `consumer_${streamName}_${consumerName}`;
    const subject = `${streamName}.${filterSubject}`;
    return {
        "durable_name": durableName,
        "filter_subject": subject,
        "deliver_policy": DeliverPolicy.All,                      // Deliver all messages
        "ack_policy": AckPolicy.Explicit,                         // Explicit ack policy
        "ack_wait": 30 * 1_000 * 1_000_000,                       // 30-second ack wait
        "replay_policy": ReplayPolicy.Original,                   // Replay original messages
    } satisfies Partial<ConsumerConfig>;
};
