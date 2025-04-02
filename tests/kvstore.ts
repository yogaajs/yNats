import { KvStore } from '@/modules/kvstore/classes/Store.class';

//  TESTS
// ===========================================================

let j = 0;  

(async () => {
    const kvStore = new KvStore({
        storeName: "readContract",
        debounceDelayMs: 250,
        cacheTtlMs: 60_000,
    });

    const keys = await kvStore.getAllKeys();
    console.log(keys);

    const value = await kvStore.get(`0x123`);
    console.log("result:", value);

    const unwatch = await kvStore.watch("0x123", 
        (revision: number, value: string) => {
            console.log(revision, value);
        }
    );

    const interval = setInterval(async () => {
        for (let i = 0; i < 12; i++) {
            kvStore.put(`0x123${i}`, { block: 1230 + j });
            j++;
        }
    }, 2_000);


    process.once("SIGINT", () => {
        clearInterval(interval);
        unwatch();
    });
    process.once("SIGTERM", () => {
        clearInterval(interval);
        unwatch();
    });

    // setInterval(async () => {
    //     kvStore.purge(`0x123`);
    // }, 2_500);
})();