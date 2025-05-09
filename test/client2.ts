import { StreamConsumer, type MessagePayload } from '@/modules';

//  TESTS
// ===========================================================

let counter = 1230;

(async () => {

    const consumer1 = new StreamConsumer({
        streamName: "EthereumBlocks",
        consumerName: "consumer-bot1",
        consumerConfig: {},
    });

    const consumer2 = new StreamConsumer({
        streamName: "EthereumBlocks",
        consumerName: "consumer-bot2",
        consumerConfig: {},
    });

    consumer1.subscribe(async (msg: MessagePayload) => {
        const diff = Date.now() - msg.timestamp;
        console.log("consumer1 message", diff);
        const delay = Math.floor(Math.random() * 10000) + 1000; // 10s max delay
        await new Promise(resolve => setTimeout(resolve, delay));
    });

    consumer2.subscribe(async (msg: MessagePayload) => {
        const diff = Date.now() - msg.timestamp;
        console.log("consumer2 message", diff);
        const delay = Math.floor(Math.random() * 10000) + 1000; // 10s max delay
        await new Promise(resolve => setTimeout(resolve, delay));
    });
})();