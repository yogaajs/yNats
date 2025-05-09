// Utilities
// ===========================================================

export const setSubject = (streamName: string, subject: string) => {
    return `${streamName}.${subject}`;
};

export const getSubject = (streamName: string, subject: string) => {
    return subject.replace(`${streamName}.`, '');
};
