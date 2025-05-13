import { Client } from '../src/index';
import { ApiRequester, ApiResponder } from '../src/modules/api';
import { instances } from './constants/instances';

// Constants
// ===========================================================

const client = new Client(instances);

const requester = new ApiRequester(client, {
    streamName: "api3",
});

const responder1 = new ApiResponder(client, {
    streamName: "api3",
    consumerName: "consumer1",
    filterSubject: "providers",
}, {
    maxConcurrent: 10,
    debug: true,
});

//  TESTS
// ===========================================================

export async function testConcurrency() {
    responder1.subscribe(
        async (subjects: any, msg: any) => {
            const payload = {
                foo: 'bar',
                data: new Array(1_000).fill('some-repeating-text') // simulate large JSON
            };
            return ({ duration: Date.now() - msg.timestamp, payload })
        },
    );
    requester.request("providers", {
        message: "Hello, world!",
    })
    .then((res) => {
        console.log(res);
    })
    .catch(console.error);

    return;

    // Test send messages and request
    await new Promise(resolve => setTimeout(resolve, 10_000));
    setInterval(async () => {
        const random = Math.floor(Math.random() * 30);
        const min = Math.max(random, 10);
        console.log("sending messages", min);

        for (let i = 0; i < min; i++) {
            requester.request("providers", {
                message: "Hello, world!",
            }).catch(console.error);
        }
    }, 2_000);

    // responder2.subscribe(
    //     async (subjects: any, msg: any) => {
    //         console.log("responder2", subjects, msg);
    //         return ({ duration: Date.now() - msg.timestamp })
    //     },
    // );

    // setTimeout(() => {
    //     responder.unsubscribe();
    // }, 10_000);
}

testConcurrency();