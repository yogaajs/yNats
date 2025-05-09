import { Client, Stream2, StreamProducer, StreamResponder, StreamRequester } from '../../src/index';
import { instances } from '../constants/instances';

//  TESTS
// ===========================================================

const client = new Client(instances);
client.initialize();

const stream1 = new Stream2({client, config: {
    name: "api",
}});

const responder1 = new StreamResponder({client, stream: stream1, config: {
    name: "consumer-bot10",
}});

const requester1 = new StreamRequester({client, stream: stream1, options: {
    maxAttempts: 2,
}});

(async () => {

    // Subscribe to the stream
    responder1.subscribe(
        async (msg: any, reply: (data: Record<string, any>) => void) => {
            // Respond to the request
            reply({ duration: Date.now() - msg.timestamp });
        },
        {
            max_messages: 1,
        },
    );

    const interval = setInterval(async () => {
        const response = await requester.request("test", {
            message: "Hello, world!",
        });
        console.log("response", response);
    }, 5_000);

})();