import { StreamProducer } from '../src/modules/jetstream/classes/Producer.class';

//  TESTS
// ===========================================================

let counter = 1230;

(async () => {
    const producer = new StreamProducer({
        streamConfig: {
            name: "EthereumBlocks",
            subjects: ["EthereumBlocks.*"],
        },
    });

    const lastMessage = await producer.getLastMessage();
    console.log("lastMessage", lastMessage);

    setTimeout(async () => {
        counter++;
        await producer.publish(counter, {
            block: counter,
            transactions: [
            {
                hash: "0x1234567890",
                timestamp: Date.now(),
            }
            ],
        });
    }, 10_000);
})();