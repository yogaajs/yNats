import type { JetStreamManager } from "nats";
import type { Logger } from "@/classes/Logger.class";
import { decodeMessage } from '../utils';

// Function
// ===========================================================

export const getMessage = async ({
    logger,
    jsm,
    streamName,
    msgId
}: {
    logger: Logger;
    jsm: JetStreamManager;
    streamName: string;
    msgId: string | number;
}) => {
    try {
        // Build the subject that corresponds to this id.
        const subject = `${streamName}.${msgId}`;

        // Retrieve the message at the given sequence
        const storedMessage = await jsm.streams.getMessage(
            streamName, 
            { 
                last_by_subj: subject 
            }
        );

        // Decode the message
        return decodeMessage(storedMessage);

    } catch (error: any) {
        logger.error(`Error retrieving message ${msgId} in stream ${streamName}:`, 
            (error as Error).message
        );
        throw error;
    }
}