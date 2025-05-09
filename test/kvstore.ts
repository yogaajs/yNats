import { Client } from '../src/index';
import { Store } from '../src/modules/store';
import { instances } from './constants/instances';

// Constants
// ===========================================================

const client = new Client(instances);

//  TESTS
// ===========================================================

let j = 0;  

(async () => {
    const kvStore = new Store(client, {
        storeName: "readContract",
        storeStorage: "memory",
        storeMaxAgeSeconds: 60,
        storeMaxMegabytes: 100,
    });

    await kvStore.getAllKeys().then((keys) => {
        console.log("getAllKeys:", keys);
    });

    let times: number[] = [];

    for (let i = 0; i < 1000; i++) {
        const start = Date.now();
        kvStore.put(`0x${i}`, { block: i }).then((value) => {
            times.push(Date.now() - start);
        });
    }
    
    let times2: number[] = [];

    for (let i = 0; i < 1000; i++) {
        const start = Date.now();
        kvStore.get(`0x${i}`).then((value) => {
            times2.push(Date.now() - start);
        });
    }

    setTimeout(() => {
        console.log("times:", times);
        console.log("times2:", times2);
    }, 3_000);
})();