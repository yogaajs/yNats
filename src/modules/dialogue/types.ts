import { NatsError, ErrorCode, RequestOptions as NatsRequestOptions } from 'nats';

// Types
// ===========================================================

export interface MessagePayload {
    timestamp: number;
    data: Record<string, any>;
}

// Requester
// ===========================================================

export interface RequestOptions extends NatsRequestOptions {
    retries?: number;
    retryDelay?: number;
}

export interface RequestPayload<T = any> {
    timestamp: number;
    data: Record<string, T>;
}

export interface ResponsePayload<T = any> {
    data: T;
    error?: {
        code: string;
        message: string;
    };
}