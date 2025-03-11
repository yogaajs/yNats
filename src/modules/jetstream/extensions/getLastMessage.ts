import type { JetStreamManager } from "nats";
import type { Logger } from "@/classes/Logger.class";
import { decodeMessage } from '../utils';

// Function
// ===========================================================

export const getLastMessage = async ({
    logger,
    jsm,
    streamName,
}: {
    logger: Logger;
    jsm: JetStreamManager;
    streamName: string;
}) => {
    try {
        // Retrieve stream information to find the last sequence number
        const streamInfo = await jsm.streams.info(streamName);
        const lastSeq = streamInfo.state.last_seq;
    
        if (!lastSeq) {
            logger.info(`No messages found in stream ${streamName}`);
            return null;
        }
    
        // Retrieve the message at the last sequence
        const storedMessage = await jsm.streams.getMessage(streamName, { 
            seq: lastSeq 
        });

        // Decode the message
        return decodeMessage(storedMessage);

    } catch (error: any) {
        logger.error(`Error retrieving last message in stream ${streamName}:`, 
            (error as Error).message
        );
        throw error;
    }
}