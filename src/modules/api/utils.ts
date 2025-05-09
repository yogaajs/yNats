import { createInbox, headers, type MsgHdrs } from '@nats-io/nats-core';

// Utilities
// ===========================================================

export const setSubject = (streamName: string, subject: string) => {
    return `${streamName}.${subject}`;
};

export const getSubject = (streamName: string, subject: string) => {
    return subject.replace(`${streamName}.`, '');
};

export const getHeader = (h: MsgHdrs): string => {
    return h.get('reply-inbox') || '';
};

export const setHeader = (inbox: string): MsgHdrs => {
    const hdr = headers();
    hdr.set('reply-inbox', inbox);
    return hdr;
};

export const setInbox = (subject: string): string => {
    return createInbox(subject);
};
