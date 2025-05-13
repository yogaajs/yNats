import snappy from 'snappy';

export async function compress<T>(obj: T, debug: boolean = false): Promise<Uint8Array> {
    const start = Date.now();
    try {
        const jsonString = JSON.stringify(obj);
        const jsonBuffer = Buffer.from(jsonString, 'utf8');
        const compressed = await snappy.compress(jsonBuffer);
        const array = new Uint8Array(compressed);

        if (debug) {
            compressionDetails(jsonBuffer, array);
        }
        return array;

    } catch (err: unknown) {
        throw new Error(
            `Snappy compression failed: ${err instanceof Error ? err.message : String(err)}`
        );
    } finally {
        if (debug) {
            const end = Date.now();
            console.log(`Compression 2 took ${end - start}ms`);
        }
    }
}

export async function decompress<T>(payload: Uint8Array, debug: boolean = false): Promise<T> {
    const start = Date.now();
    try {
        const input = Buffer.from(payload); // Convertit Uint8Array en Buffer
        const decompressed = await snappy.uncompress(input);
        const text = decompressed.toString();

        return JSON.parse(text);

    } catch (err: unknown) {
        throw new Error(
            `Snappy decompression failed: ${err instanceof Error ? err.message : String(err)}`
        );
    } finally {
        if (debug) {
            const end = Date.now();
            console.log(`Decompression took ${end - start}ms`);
        }
    }
}

export function compressionDetails(before: Buffer, after: Uint8Array): void {
    const toMB = (bytes: number): number => bytes / (1024 * 1024);

    console.log('Original size:', toMB(before.length).toFixed(3), 'MB');
    console.log('Compressed size:', toMB(after.length).toFixed(3), 'MB');

    const ratio = (after.length / before.length) * 100;
    console.log(`Data was compressed to ${ratio.toFixed(2)}% of its original size.`);
}