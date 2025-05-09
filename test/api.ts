import { Client } from '../src/index';
import { StreamRequester, StreamResponder } from '../src/modules/api';
import { instances } from './constants/instances';

// Constants
// ===========================================================

const client = new Client(instances);

const requester = new StreamRequester(client, {
    streamName: "api3",
    streamConfig: {},
});

//  TESTS
// ===========================================================

export async function testConcurrency() {

    const responder1 = new StreamResponder(client, {
        streamName: "api3",
        streamConfig: {},
        consumerName: "consumer1",
        consumerConfig: {},
        filterSubject: "providers",
    });

    const responder2 = new StreamResponder(client, {
        streamName: "api3",
        streamConfig: {},
        consumerName: "consumer2",
        consumerConfig: {},
        filterSubject: "explorers",
    });

    // Test send messages and request
    setInterval(async () => {
        const randomNumber1 = Math.floor(Math.random() * 10);
        for (let i = 0; i < randomNumber1; i++) {
            requester.request("providers", {
                message: "Hello, world!",
            }).then((response) => {
                console.log("response", response);
            });
        }
    }, 1_000);

    responder1.subscribe(
        async (subjects: any, msg: any) => {
            console.log("responder1", subjects, msg);
            return ({ duration: Date.now() - msg.timestamp })
        },
    );

    responder2.subscribe(
        async (subjects: any, msg: any) => {
            console.log("responder2", subjects, msg);
            return ({ duration: Date.now() - msg.timestamp })
        },
    );

    // setTimeout(() => {
    //     responder.unsubscribe();
    // }, 10_000);
}

testConcurrency();