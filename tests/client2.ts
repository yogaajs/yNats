import { StreamProducer } from '@/modules/jetstream/classes/Producer.class';
import { StreamConsumer } from '@/modules/jetstream/classes/Consumer.class';
import { RetentionPolicy, StorageType, JsMsg } from 'nats';

//  TESTS
// ===========================================================

let counter = 1230;

(async () => {

    const consumer1 = new StreamConsumer({
        streamName: "EthereumBlocks",
        consumerName: "consumer-bot1",
    });

    const consumer2 = new StreamConsumer({
        streamName: "EthereumBlocks",
        consumerName: "consumer-bot2",
    });

    consumer1.subscribe(async (msg: JsMsg) => {
        console.log("consumer1 message:", msg);
        await new Promise(resolve => setTimeout(resolve, 5000));
        console.log("consumer1 message:", "done");
    });

    // consumer2.subscribe(async (msg: JsMsg) => {
    //     console.log("consumer2 message:", msg);
    // });
})();