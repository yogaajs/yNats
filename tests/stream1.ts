import { Client } from '../src/core/client.class';
import { StreamPublisher, StreamConsumer } from '../src/modules/stream';
import { instances } from './constants/instances';
import { RetentionPolicy, StorageType } from '@nats-io/jetstream';

// Constants
// ===========================================================

const client = new Client(instances);

const publisher = new StreamPublisher(client, {
    streamName: "stream1",
    streamRetention: RetentionPolicy.Workqueue,
    streamStorage: StorageType.Memory,
    streamConfig: {
        max_bytes: 1024 * 1024 * 1024, // 1GB
    },
});

const consumer = new StreamConsumer(client, {
    streamName: "stream1",
    streamRetention: RetentionPolicy.Workqueue,
    streamStorage: StorageType.Memory,
    streamConfig: {
        max_bytes: 1024 * 1024 * 1024, // 1GB
    },
    consumerName: "consumer1",
    consumerConfig: {
    },
});

//  TESTS
// ===========================================================

export async function testConcurrency() {

    // Test send messages and request
    setTimeout(async () => {
        const randomNumber1 = Math.floor(Math.random() * 100);
        for (let i = 0; i < randomNumber1; i++) {
            publisher.publish("test", {
                message: "Hello, world " + Date.now(),
            })
        }
        publisher.shutdown();
    }, 1_000);

    consumer.subscribe(
        async (subject: string, payload: IPayload) => {
            const start = Date.now();
            const randomDelay = Math.floor(Math.random() * 100);
            await new Promise(resolve => setTimeout(resolve, randomDelay));
            console.log("consumer", subject, payload, Date.now() - start);
        },
    );

    setTimeout(() => {
        consumer.unsubscribe();
        publisher.shutdown();
    }, 15_000);
}

testConcurrency();