import type { MessagePayload } from '../types';
import { Client } from 'src/core/Client';

import { Readiness } from 'src/classes/Readiness.class';
import { Logger } from 'src/classes/Logger.class';

// Class
// ===========================================================

export class StreamCommon extends Client {
    private readonly _streamName: string;
    private readonly _streamLogger: Logger;
    private readonly _streamReady: Readiness;

    constructor(className: string, streamName: string) {
        if (!streamName) {
            throw new Error("Stream name is required");
        }

        super();

        this._streamName = streamName;
        this._streamLogger = new Logger(`[jetstream][${className}][${streamName}]`);
        this._streamReady = new Readiness();
    }

    // Protected

    protected get streamName(): string {
        return this._streamName;
    }

    protected get streamReady(): Readiness {
        return this._streamReady;
    }

    protected get streamLogger(): Logger {
        return this._streamLogger;
    }

    // Public

    public async getMessage(msgId: string | number): Promise<MessagePayload['data'] | null> {
        await super.clientReady.isReady();
        await this.streamReady.isReady();
        try {
            // Build the subject that corresponds to this id.
            const subject = `${this._streamName}.${msgId}`;
    
            // Retrieve the message at the given sequence
            const storedMessage = await super.jetstreamManager.streams.getMessage(
                this._streamName, 
                { last_by_subj: subject }
            );

            // Message not found
            if (!storedMessage) { return null; }
    
            // Decode the message
            return storedMessage.json() as MessagePayload;
    
        } catch (error: any) {
            this._streamLogger.error(`Error retrieving message ${msgId} in stream ${this._streamName}:`, 
                (error as Error).message
            );
            throw error;
        }
    }

    public async getLastMessage(): Promise<MessagePayload['data'] | null> {
        await super.clientReady.isReady();
        await this.streamReady.isReady();
        try {
            // Retrieve stream information to find the last sequence number
            const streamInfo = await super.jetstreamManager.streams.info(this._streamName);
            const lastSeq = streamInfo.state.last_seq;
        
            if (!lastSeq) {
                this._streamLogger.info(`No messages found in stream ${this._streamName}`);
                return null;
            }
        
            // Retrieve the message at the last sequence
            const storedMessage = await super.jetstreamManager.streams.getMessage(
                this._streamName, 
                { seq: lastSeq }
            );
    
            if (!storedMessage) {
                // Message not found
                return null;
            }

            // Decode the message
            return storedMessage.json() as MessagePayload;
    
        } catch (error: any) {
            this._streamLogger.error(`Error retrieving last message in stream ${this._streamName}:`, 
                (error as Error).message
            );
            throw error;
        }
    }
}
