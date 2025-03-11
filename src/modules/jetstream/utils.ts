import { StringCodec, type StoredMsg } from "nats";
import type { MessagePayload } from './types';

// Class
// ===========================================================

export const sc = StringCodec();

export const decodeMessage = (msg: StoredMsg) => {
    const stringifiedMessage = sc.decode(msg.data);
    const message = JSON.parse(stringifiedMessage) as MessagePayload;
    return message.data;
}