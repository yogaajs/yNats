import { StreamProducer, StreamConsumer } from '@/modules/jetstream';
import { MessagePayload } from '@/modules/jetstream/types';

//  TESTS
// ===========================================================

const blocks = [
    {
        block: 1230,
        timestamp: Date.now(),
    },
    {
        block: 1231,
        timestamp: Date.now(),
    },
    {
        block: 1232,
        timestamp: Date.now(),
    },
    {
        block: 1233,
        timestamp: Date.now(),
    },
    {
        block: 1234,
        timestamp: Date.now(),
    },
    {
        block: 1235,
        timestamp: Date.now(),
    },
    {
        block: 1236,
        timestamp: Date.now(),
    },
    {
        block: 1237,
        timestamp: Date.now(),
    },
];

const producer = new StreamProducer({
    streamConfig: {
        name: "EthereumBlocks",
        subjects: ["EthereumBlocks.*"],
    },
});

const consumer = new StreamConsumer({
    streamName: "EthereumBlocks",
    consumerName: "consumer-bot1",
    consumerConfig: {},
});

(async () => {
    for (const block of blocks) {   
        await producer.publish(block.block, { data: block });
    }

    consumer.subscribe(async (msg: MessagePayload) => {
        const diff = Date.now() - msg.timestamp;
        console.log("consumer1 message", diff);
        await new Promise(resolve => setTimeout(resolve, 100));
    });

    const interval = setInterval(async () => {
        for (const block of blocks) {   
            await producer.publish(block.block, { data: block });
        }
    }, 3_000);

    process.once("SIGINT", () => {
        clearInterval(interval);
        consumer.unsubscribe();
    });
    process.once("SIGTERM", () => {
        clearInterval(interval);
        consumer.unsubscribe();
    });
})();