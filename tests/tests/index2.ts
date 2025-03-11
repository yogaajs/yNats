import { StreamProducer } from '@/modules/jetstream/Producer.class';
import options from 'src/constants/instances';

//  TEST
// ==========================================================

let blockNum = 0;

(async () => {
    const producer = new StreamProducer({
        options: options.localhost["6379"], 
        streamName: "stream-block-ethereum",
        debounceMs: 100,
        maxEntries: 150
    });


    const lastData = await producer.getAllData();
    console.log(lastData);

    setInterval(async () => {
        blockNum += 1; // Increment by 1
        await producer.publish({ data: { test: 'test' }, test: 'test' }, false);
        console.log(`Published directly ethereum: ${blockNum}`);
    }, 3000);

    setInterval(async () => {
        blockNum += 1;
        const publish1 = producer.publish({ data: blockNum, test: 'test' }, true);
        blockNum += 1;
        const publish2 = producer.publish({ data: blockNum, test: 'test' }, true);
        blockNum += 1;
        const publish3 = producer.publish({ data: blockNum, test: 'test' }, true);
        
        await Promise.all([publish1, publish2, publish3]);
        console.log(`Published batch ethereum: ${blockNum}`);
    }, 2000);
})();