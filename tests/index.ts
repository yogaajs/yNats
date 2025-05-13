//  Global
// ===========================================================

const MAX_CALLS = 200;
const MAX_CALLS_PER_FUNCTION = 0.25 * MAX_CALLS; // 25% max

var CONCURRENT_CALLS = 0;

//  Local
// ===========================================================

const CALLS_PER_REQUEST = 3;
const MAX_CONCURRENT_REQUESTS = Math.floor(MAX_CALLS_PER_FUNCTION / CALLS_PER_REQUEST); // Max requests

const runningRequests = new Set<Promise<any>>();


//  Function
// ===========================================================

async function concurrent(runningRequests, request) {
    var maxRequests = () => runningRequests.size + 1 > MAX_CONCURRENT_REQUESTS
    var maxCalls = () => CONCURRENT_CALLS + request.count > MAX_CALLS

    // Wait until there's capacity for both a new request and its "count"
    if (runningRequests.size > 0) {
        while (
            maxRequests() ||    // Permet de limiter a 25% du max 
            maxCalls()          // Permet d'éviter que plusieurs soit au max (25%) et que lui aussi y aille
        ) {
            // Wait until one of the running request promises settles
            await Promise.race(runningRequests);
        }
    }
}

async function handler(request) {
    // Wait until there's capacity for both a new request and its "count"
    await concurrent(runningRequests, request);

    // Start processing the request
    const promise = request.handlers
        .then((result) => {
            request.resolve(result);
        })
        .catch((error) => {
            request.reject(error);
        })
        .finally(() => {
            runningRequests.delete(promise);
            CONCURRENT_CALLS -= request.count;
        });

    runningRequests.add(promise);
    CONCURRENT_CALLS += request.count;
}