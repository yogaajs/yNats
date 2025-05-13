import snappy from 'snappy';

export async function compress(input: string, debug: boolean = false): Promise<Uint8Array> {
    const start = Date.now();
    try {
        const jsonBuffer = Buffer.from(input, 'utf8');
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

export async function decompress(payload: Uint8Array, debug: boolean = false): Promise<string> {
    const start = Date.now();
    try {
        const buffer = Buffer.from(payload); // Convertit Uint8Array en Buffer
        const decompressed = await snappy.uncompress(buffer);
        const input = decompressed.toString();

        return input;

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