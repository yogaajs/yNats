import { createInbox, headers, type MsgHdrs } from '@nats-io/nats-core';

// Subject
// ===========================================================

export const setSubject = (streamName: string, subject: string) => {
    return `${streamName}.${subject}`;
};

export const getSubject = (streamName: string, subject: string) => {
    return subject.replace(`${streamName}.`, '');
};

// Header
// ===========================================================

export const setHeader = (inbox: string): MsgHdrs => {
    const hdr = headers();
    hdr.set('reply-inbox', inbox);
    return hdr;
};

export const getHeader = (h: MsgHdrs): string => {
    return h.get('reply-inbox') || '';
};

// Inbox
// ===========================================================

export const setInbox = (subject: string): string => {
    return createInbox(subject);
};

// Payload
// ===========================================================

export const createPayload = (data: Record<string, any>): string => {
    return JSON.stringify({ timestamp: Date.now(), ...data });
};

export const parsePayload = (payload: string): Record<string, any> => {
    return JSON.parse(payload);
};
