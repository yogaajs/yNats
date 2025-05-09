import { Client, Stream2, StreamProducer, StreamResponder, StreamRequester } from '../../src/index';
import { instances } from '../constants/instances';

//  TESTS
// ===========================================================

const client = new Client(instances);
client.initialize();

const stream1 = new Stream2({client, config: {
    name: "api",
}});

const requester1 = new StreamRequester({client, stream: stream1, options: {
    maxAttempts: 2,
}});

(async () => {

    const interval = setInterval(async () => {
        const response = await requester1.request("test", {
            message: "Hello, world!",
        });
        console.log("response", response);
    }, 5_000);

})();