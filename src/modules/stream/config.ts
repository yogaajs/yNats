import type { StreamConfig, ConsumerConfig } from '@nats-io/jetstream';
import { AckPolicy, DeliverPolicy, DiscardPolicy, ReplayPolicy } from '@nats-io/jetstream';
import type { StreamPublisher } from './modules/publisher.class';
import type { StreamConsumer } from './modules/consumer.class';

// Stream
// ===========================================================

export const streamConfig = ({
    streamName,
    streamRetention,
    streamStorage,
    streamMaxConsumers,
    streamMaxAgeSeconds,
    streamMaxMegabytes,
}: StreamPublisher.Config) => {
    const maxConsumers = (streamMaxConsumers ?? 10);
    const maxAgeSeconds = (streamMaxAgeSeconds ?? 6 * 60 * 60);
    const maxMegabytes = (streamMaxMegabytes ?? 512);
    return {
        "name": streamName,                           // Set the name
        "subjects": [`${streamName}.>`],              // Set the subjects
        "retention": streamRetention,                 // Set the retention
        "storage": streamStorage,                     // Set the storage
        "discard": DiscardPolicy.Old,                 // Set the discard policy
        "max_msgs": 10_000,                           // Max messages in the stream
        "max_msgs_per_subject": 1_000,                // Per subject retention limit
        "max_consumers": maxConsumers,                // Max consumers for this stream
        "max_age": maxAgeSeconds * 1_000_000_000,     // Nanoseconds
        "max_bytes": maxMegabytes * 1024 * 1024,      // MB
    } satisfies Partial<StreamConfig>;
};

// Consumer
// ===========================================================

export const consumerConfig = ({
    streamName,
    streamSubject,
    consumerName,
}: Pick<StreamConsumer.Config, 'streamName' | 'streamSubject' | 'consumerName'>) => {
    const durableName = `consumer_${streamName}_${consumerName}`;
    const filterSubject = streamSubject ? `${streamName}.${streamSubject}` : undefined;
    return {    
        "durable_name": durableName,
        "filter_subject": filterSubject,
        "deliver_policy": DeliverPolicy.All,                      // Deliver all messages
        "ack_policy": AckPolicy.Explicit,                         // Explicit ack policy
        "ack_wait": 30 * 1_000 * 1_000_000,                       // 30-second ack wait
        "replay_policy": ReplayPolicy.Original,                   // Replay original messages
    } satisfies Partial<ConsumerConfig>;
};
