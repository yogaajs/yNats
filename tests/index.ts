import { PublicClientWss, PublicClientHttp } from '../src/clients/public';
import type { RpcBlock } from 'viem';

//  Class
// ===========================================================

const providers: any = {
    sonic: {
        http: "https://go.getblock.io/f503f2e842a54f3faad1c3e41cd4ca84",
        wss: "wss://go.getblock.io/1d68b02f8c2f4e7d90e649022ac6d544",
        type: 'debug',
    },
    avalanche: {
        http: "https://go.getblock.io/9b3e52b28d374a4683b0e4215a9f1f4d/ext/bc/C/rpc",
        wss: "wss://go.getblock.io/547438b0d74d4c1389351122ab8bd802/ext/bc/C/rpc",
        type: 'debug',
    }
};

async function testClient(client: PublicClientWss | PublicClientHttp) {
    try {
        const blockNumber = await client.client.getBlockNumber();
        console.log(blockNumber);
    } catch (error) {
        console.error(error);
    }
}

(async function() {  
    // Execute

    const wss = new PublicClientWss({
        chainId: 43114,
        provider: providers.sonic,
    });

    const http = new PublicClientHttp({
        chainId: 43114,
        provider: providers.sonic,
    });

    // Fetch traces block
    testClient(http);
    testClient(wss);

    wss.subscribe.newHeads.start({
        onData: (block: RpcBlock) => {
            console.log(block.number);
        },
        onError: (error: Error) => {
            console.error(error);
        }
    });
})()
