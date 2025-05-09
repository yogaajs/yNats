import { StreamProducer } from '@/modules/jetstream/classes/Producer.class';
import { StreamConsumer } from '@/modules/jetstream/classes/Consumer.class';
import { JsMsg } from 'nats';

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
    console.log(lastMessage);

    setInterval(async () => {
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
    }, 2_000);
})();