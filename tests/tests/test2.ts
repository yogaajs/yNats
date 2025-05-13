import RedisConsumer, { MessagePayload } from '../classes/consumer.class';
import { Streams, Groups, Consumers} from 'src/constants/streams';
import options from 'src/constants/instances';


//  TESTS
// ===========================================================

const randomWait = (minMs: number, maxMs: number) => 
    new Promise(resolve => setTimeout(resolve, Math.floor(Math.random() * (maxMs - minMs + 1)) + minMs));

const processFn = async (payload: MessagePayload[]) => {
    for (const element of payload) {
        const { blockNum, chain } = element.data;
        console.log(`Processing block: ${blockNum} for ${chain}`, Date.now() - element.timestamp);
        await randomWait(10, 400);
    }
}

const consumer = new RedisConsumer({ 
    options: options.evm, 
    streamName: Streams.ETHEREUM,
    groupName: Groups.INDEXER,
    consumerName: Consumers.INDEXER_2,
    processFn
});

(async () => {
    await consumer.xReadGroup();
})();
