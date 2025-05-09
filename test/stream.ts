import { Client, Stream, StreamConsumer, StreamProducer } from '../src/index';
import { instances } from './constants/instances';

//  TESTS
// ===========================================================

const client = new Client(instances);
client.initialize();

const stream = new Stream({
    client,
    options: {
        name: "EthereumBlocks",
    },
});

const producer = new StreamProducer({client, stream, options: {
    maxAttempts: 2,
}});

const consumer = new StreamConsumer({client, stream, options: {
    name: "consumer-bot10",
    config: {},
}});

(async () => {

    // Subscribe to the stream
    consumer.subscribe(
        async (msg: any) => {
            console.log("message", msg);
            const randomDelay = Math.floor(Math.random() * 2_000);
            await new Promise(resolve => setTimeout(resolve, randomDelay));
        },
        {
            max_messages: 1,
        },
    );

    let count = 0;

    const interval = setInterval(async () => {
        await producer.publish("test", {
            message: "Hello, world!",
        });
        count++;
        if (count === 15) {
            clearInterval(interval);
        }
    }, 1000);

    setTimeout(async () => {
        client.shutdown();
    }, 5_000);

})();